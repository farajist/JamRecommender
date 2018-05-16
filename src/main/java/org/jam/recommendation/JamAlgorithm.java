package org.jam.recommendation;

import org.apache.spark.api.java.Optional;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.mahout.math.cf.DownsamplableCrossOccurrenceDataset;
import org.apache.mahout.math.cf.ParOpts;
import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.predictionio.controller.java.P2LJavaAlgorithm;
import org.apache.predictionio.data.storage.Event;
import org.apache.predictionio.data.storage.NullModel;
import org.apache.predictionio.data.store.java.LJavaEventStore;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.jam.recommendation.util.ids.IndexedDatasetJava;
import org.jam.recommendation.util.similarity.SimilarityAnalysisJava;
import org.joda.time.DateTime;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.stream.Collectors;

public class JamAlgorithm extends P2LJavaAlgorithm<PreparedData, NullModel, Query, PredictedResult> {


    private static final Logger logger = LoggerFactory.getLogger(JamAlgorithm.class);
    private final JamAlgorithmParams ap;

    private final String appName;
    private final String esIndex;
    private final String esType;
    private final String recsModel;
    private final List<String> dateNames;

//    private final List<String> eventNames;
    private final List<String> blacklistEvents;
    private final List<Field> fields;
    private final List<RankingParams> rankingParams;
//    private final List<IndicatorParams> indicators;
    private final List<String> modelEventNames = new ArrayList<>();

    private final Integer maxQueryEvents;
    private final Integer maxEventsPerEventType;
    private final Integer maxCorrelatorsPerEventType;
//    private final Integer num;
    private final Integer randomSeed;
    private final Integer limit;

    private final Float userBias;
    private final Float itemBias;

    private final Boolean returnSelf;

//    private final Long seed;
    private final List<String> rankingFieldNames;

    //used in getBiasedUserActions
    private List<String> queryEventNames = new ArrayList<>();
    private List<Tuple2<String, Object>> parameters = new ArrayList<>();

    /**
     * creates coocurrence, cross-coocurrence and eventually correlators with
     * Mahout's similarity analysis. The analysis part of the recommender is done
     * here but the algorithm can predict only when the coocurrence data is indexed
     * in a search engine like ElasticSearch. This is done through JamModel.save
     *
     * @param ap taken from engine.json to describe limits and event types
     */
    public JamAlgorithm(JamAlgorithmParams ap) {
        this.ap = ap;
        this.appName = ap.getAppName();
        this.recsModel = ap.getRecsModelOrElse(DefaultJamAlgorithmParams.DEFAULT_RECS_MODEL);
        this.userBias = ap.getUserBiasOrElse(1f);
        this.itemBias = ap.getItemBiasOrElse(1f);
        this.maxQueryEvents = ap.getMaxQueryEventsOrElse(DefaultJamAlgorithmParams.DEFAULT_MAX_QUERY_EVENTS);
        this.limit = ap.getNumOrElse(DefaultJamAlgorithmParams.DEFAULT_NUM);
        this.blacklistEvents = ap.getBlacklistEvents();
        this.returnSelf = ap.getReturnSelfOrElse(DefaultJamAlgorithmParams.DEFAULT_RETURN_SELF);
        this.fields = ap.getFields();
        this.randomSeed = ap.getSeedOrElse(System.currentTimeMillis()).intValue();
        this.maxCorrelatorsPerEventType = ap.getMaxCorrelatorsPerEventTypeOrElse(
                DefaultJamAlgorithmParams.DEFAULT_MAX_CORRELATORS_PER_EVENT_TYPE
        );

        this.maxEventsPerEventType = ap.getMaxEventsPerEventTypeOrElse(
                DefaultJamAlgorithmParams.DEFAULT_MAX_EVENTS_PER_EVENT_TYPE
        );

        if (ap.getIndicators() == null) {
            if (ap.getEventNames() != null) {
                throw new IllegalArgumentException("No eventNames or indicators in engine.json" +
                        " and one of these is required");
            } else {
                this.modelEventNames.addAll(ap.getEventNames());
            }
        } else {
            for (IndicatorParams ip : ap.getIndicators()) {
                this.modelEventNames.add(ip.getName());
            }
        }

        List<RankingParams> defaultRankingParams = new ArrayList<>(Collections.singletonList(
                new RankingParams(
                        DefaultJamAlgorithmParams.DEFAULT_BACKFILL_FIELD_NAME,
                        DefaultJamAlgorithmParams.DEFAULT_BACKFILL_TYPE,
                        this.modelEventNames.subList(0,1),
                        null,
                        null,
                        DefaultJamAlgorithmParams.DEFAULT_BACKFILL_DURATION
                )
        ));

        this.rankingParams = ap.getRankingsOrElse(defaultRankingParams);
        this.rankingParams.sort(new RankingParamsComparatorByGroup());
        /**
         * the popularity model stores a map of ranktype/rankfieldname
         * we use it to get fieldnames for ranking, this list is useful
         * for the predict method
         */
        this.rankingFieldNames = this.rankingParams.stream()
                .map(rankingParams -> {
                    String rankingType = rankingParams.getBackfillTypeOrElse(
                            DefaultJamAlgorithmParams.DEFAULT_BACKFILL_TYPE
                    );
                    return rankingParams.getNameOrElse(
                            PopModel.nameByType.get(rankingType)
                    );
                }).collect(Collectors.toList());
        this.dateNames = new ArrayList<>(Arrays.asList(
                ap.getDateName(),
                ap.getAvailableDateName(),
                ap.getExpireDateName()
        )).stream().distinct().collect(Collectors.toList());
        this.esIndex = ap.getIndexName();
        this.esType = ap.getTypeName();

        this.logJamAlgorithmParameters();
    }

    private void logJamAlgorithmParameters() {
        parameters.add(new Tuple2<>("App name", appName));
        parameters.add(new Tuple2<>("ES index name", esIndex));
        parameters.add(new Tuple2<>("ES type name", esType));
        parameters.add(new Tuple2<>("Recs Model", recsModel));
        parameters.add(new Tuple2<>("Event Names", String.join(",", modelEventNames)));
        parameters.add(new Tuple2<>("==========", "============================"));
        parameters.add(new Tuple2<>("Random seed", randomSeed));
        parameters.add(new Tuple2<>("MaxCorrelatorsPerEventType", maxCorrelatorsPerEventType));
        parameters.add(new Tuple2<>("MaxCorrelatorsPerEventType", maxEventsPerEventType));
        parameters.add(new Tuple2<>("User bias", userBias));
        parameters.add(new Tuple2<>("Item bias", itemBias));
        parameters.add(new Tuple2<>("Max query events", maxQueryEvents));
        parameters.add(new Tuple2<>("Limit", limit));
        parameters.add(new Tuple2<>("==========", "============================"));
        parameters.add(new Tuple2<>("Rankings", rankingParams.stream().map(rankingParams -> rankingParams.getName() + " ")));
    }

    class RankingParamsComparatorByGroup implements Comparator<RankingParams> {

        @Override
        public int compare(RankingParams o1, RankingParams o2) {
            int groupComaprison = o1.getBackfillType()
                    .compareTo(o2.getBackfillType());
            return groupComaprison == 0
                    ? o1.getName().compareTo(o2.getName())
                    : groupComaprison;
        }
    }

    private static class BoostableCorrelators {
        protected final String actionName;
        protected final List<String> itemIDs;
        public final Float boost;

        public BoostableCorrelators(String actionName, List<String> itemIDs,
                                    Float boost) {
            this.actionName = actionName;
            this.itemIDs = itemIDs;
            this.boost = boost;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof BoostableCorrelators) {
                BoostableCorrelators other = (BoostableCorrelators) o;
                return actionName.equals(other.actionName) &&
                        itemIDs.equals(other.itemIDs) &&
                        boost.equals(other.boost);
            }
            return false;
        }

        @Override
        public int hashCode() {

            return actionName.hashCode() + itemIDs.hashCode() + boost.hashCode();
        }

        protected FilterCorrelators toFilterCorrelators() {
            return new FilterCorrelators(this.actionName, this.itemIDs);
        }
    }

    private static class FilterCorrelators {
        protected final String actionName;
        protected final List<String> itemIDs;

        protected FilterCorrelators(String actionName, List<String> itemIDs) {
            this.actionName = actionName;
            this.itemIDs = itemIDs;
        }
    }

    private NullModel calcAll(SparkContext sc, PreparedData preparedData,
                              Boolean calcPopular) {
        //if data is empty throw an exception
        if (preparedData.getActions().size() == 0 ||
                preparedData.getActions().get(0)._2().getRowIds().size() == 0) {
            throw new RuntimeException("|Thre are no users with th primary / conversion event " +
                    "and this is not allowed" + " | Check to see that your dataset contains the primary event.");
        }

        logger.info("Actions read creating correlators");
        List<IndexedDataset> cooccurrenceIDs;
        List<IndexedDatasetJava> iDs = new ArrayList<>();
        //simple copy to local iDs, which is also an IndexedDatasetJava
        for (Tuple2<String, IndexedDatasetJava> p : preparedData.getActions()) {
            iDs.add(p._2());
        }

        if (ap.getIndicators().size() == 0) {
            //FIXME: yeah it builds cooc ids, but why the pain, check ap
            cooccurrenceIDs = SimilarityAnalysisJava.cooccurrencesIDSs(
                    iDs.toArray(new IndexedDataset[iDs.size()]),
                    //random seed
                    ap.getSeed() == null ? (int) System.currentTimeMillis() : ap.getSeed().intValue(),
                    //maxInterestingItemsPerThing
                    ap.getMaxCorrelatorsPerEventType() == null ? DefaultJamAlgorithmParams.DEFAULT_MAX_CORRELATORS_PER_EVENT_TYPE
                            : ap.getMaxCorrelatorsPerEventType(),
                    //maxNumInteractions
                    ap.getMaxEventsPerEventType() == null ? DefaultJamAlgorithmParams.DEFAULT_MAX_EVENTS_PER_EVENT_TYPE
                            : ap.getMaxEventsPerEventType(),
                    defaultParOpts()
            );
        } else {
            //in case we had indicators .. eventNames ?
            //using params per matrix pair, these take the place of eventNames, maxCorrelatorsPerEventType,
            // and maxEventsPerEventType
            List<IndicatorParams> indicators = ap.getIndicators();
            List<DownsamplableCrossOccurrenceDataset> datasets = new ArrayList<>();
            for (int i = 0; i < iDs.size(); ++i) {
                datasets.add(
                        new DownsamplableCrossOccurrenceDataset(
                                iDs.get(i),
                                indicators.get(i).getMaxItemsPerUser() == null ? DefaultJamAlgorithmParams.DEFAULT_MAX_EVENTS_PER_EVENT_TYPE
                                        : indicators.get(i).getMaxItemsPerUser(),
                                indicators.get(i).getMaxCorrelatorsPerItem() == null ? DefaultJamAlgorithmParams.DEFAULT_MAX_CORRELATORS_PER_EVENT_TYPE
                                        : indicators.get(i).getMaxCorrelatorsPerItem(),
                                OptionHelper.some(indicators.get(i).getMinLLR()),
                                OptionHelper.some(defaultParOpts())
                        )
                );
            }
            /*
                we get a List<IndexedDataset> which directly maps to scala's equivalent
             */
            cooccurrenceIDs = SimilarityAnalysisJava.crossOccurrenceDownsampled(
                    datasets,
                    ap.getSeed() == null ? (int) System.currentTimeMillis() : ap.getSeed().intValue());
        }

        /**
         * Creating a list of correlators, it has a tuple(actionName, cooc-value)
         * @param actionName brought from the preparedData directly
         * @param
         */

        List<Tuple2<String, IndexedDataset>> coocurrenceCorrelators = new ArrayList<>();
        for (int i = 0; i < cooccurrenceIDs.size(); ++i) {
            coocurrenceCorrelators.add(new Tuple2<>(
                    preparedData.getActions().get(i)._1(),
                    cooccurrenceIDs.get(i)
            ));
        }

        JavaPairRDD<String, HashMap<String, JsonAST.JValue>> propertiesRDD;
        if (calcPopular) {
            JavaPairRDD<String, HashMap<String, JsonAST.JValue>> ranksRdd = getRanksRDD(preparedData.getFieldsRDD(), sc);
            propertiesRDD = preparedData.getFieldsRDD()
                    .fullOuterJoin(ranksRdd)
                    .mapToPair(new CalcAllFunction());
        } else {
            propertiesRDD = RDDUtils.getEmptyPairRDD(sc);
        }

        logger.info("Correlators created now putting into JamModel");

        //singleton list for propertiesRDD
        ArrayList<JavaPairRDD<String, HashMap<String, JsonAST.JValue>>> pList = new ArrayList<>();
        pList.add(propertiesRDD);
        new JamModel(
                coocurrenceCorrelators,
                pList,
                getRankingMapping(),
                false,
                sc).save(dateNames, esIndex, esType);
        return new NullModel();
    }

    private Map<String, String> getRankingMapping() {
        HashMap<String, String> out = new HashMap<>();
        for (String r : rankingFieldNames) {
            out.put(r, "float");
        }
        return out;
    }

    /**
     * Lambda expression class for calcPopular in calcAll()
     */

    private static class CalcAllFunction implements
            PairFunction<
                    Tuple2<String, Tuple2<Optional<HashMap<String, JsonAST.JValue>>,
                            Optional<HashMap<String, JsonAST.JValue>>>>,
                    String,
                    HashMap<String, JsonAST.JValue>
                    > {
        //Okay the following function returns the first param in that PairFunc above
        @Override
        public Tuple2<String, HashMap<String, JsonAST.JValue>> call(
                Tuple2<String, Tuple2<Optional<HashMap<String, JsonAST.JValue>>,
                        Optional<HashMap<String, JsonAST.JValue>>>> t) {
            String item = t._1();
            Optional<HashMap<String, JsonAST.JValue>> oFieldsPropMap = t._2()._1();
            Optional<HashMap<String, JsonAST.JValue>> oRankPropMap = t._2()._2();

            if (oFieldsPropMap.isPresent() && oRankPropMap.isPresent()) {
                HashMap<String, JsonAST.JValue> fieldPropMap = oFieldsPropMap.get();
                HashMap<String, JsonAST.JValue> rankPropMap = oRankPropMap.get();
                HashMap<String, JsonAST.JValue> newMap = new HashMap<>(fieldPropMap);
                newMap.putAll(rankPropMap);
                return new Tuple2<>(item, newMap);
            } else if (oFieldsPropMap.isPresent()) {
                return new Tuple2<>(item, oFieldsPropMap.get());
            } else if (oRankPropMap.isPresent()) {
                return new Tuple2<>(item, new HashMap<String, JsonAST.JValue>());
            } else {
                return new Tuple2<>(item, new HashMap<String, JsonAST.JValue>());
            }
        }
    }

    /**
     * Lambda expression class for getRanksRDDs()
     */
    private static class RankFunction implements
            PairFunction<
                    Tuple2<String, Tuple2<Optional<HashMap<String, JsonAST.JValue>>, Optional<Double>>>,
                    String,
                    HashMap<String, JsonAST.JValue>> {

        private String fieldName;
        protected RankFunction(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public Tuple2<String, HashMap<String, JsonAST.JValue>> call(
                Tuple2<String, Tuple2<Optional<HashMap<String, JsonAST.JValue>>,
                        Optional<Double>>> t) throws Exception {
            String itemID = t._1();
            Optional<HashMap<String, JsonAST.JValue>> oPropMap = t._2()._1();
            Optional<Double> oRank = t._2()._2();

            if (oPropMap.isPresent() && oRank.isPresent()) {
                HashMap<String, JsonAST.JValue> propMap = oPropMap.get();
                HashMap<String, JsonAST.JValue> newMap = new HashMap<>(propMap);
                newMap.put(fieldName, new JsonAST.JDouble(oRank.get()));
                return new Tuple2<>(itemID, newMap);
            } else {
                return new Tuple2<>(itemID, new HashMap<String, JsonAST.JValue>());
            }
        }
    }

    private ParOpts defaultParOpts() {
        return new ParOpts(-1, -1, true);
    }

    private JavaPairRDD<String, HashMap<String, JsonAST.JValue>> getRanksRDD(
            JavaPairRDD<String, HashMap<String, JsonAST.JValue>> fieldsRdd,
            SparkContext sc) {
        PopModel popModel = new PopModel(fieldsRdd, sc);
        List<Tuple2<String, JavaPairRDD<String, Double>>> rankRDDs = new ArrayList<>();
        for (RankingParams rp : rankingParams) {
            String rankingType = rp.getBackfillType() == null ? DefaultJamAlgorithmParams.DEFAULT_BACKFILL_TYPE
                    : rp.getBackfillType();
            String rankingFieldName = rp.getName() == null ? PopModel.nameByType.get(rankingType)
                    : rp.getName();
            String durationAsString = rp.getDuration() == null ? DefaultJamAlgorithmParams.DEFAULT_BACKFILL_DURATION
                    : rp.getDuration();

            Integer duration = (int) Duration.apply(durationAsString).toSeconds();
            List<String> backfillEvents = rp.getEventNames() == null ? modelEventNames.subList(0, 1)
                    : rp.getEventNames();

            String offsetDate = rp.getOffsetDate();
            JavaPairRDD<String, Double> rankRdd = popModel.calc(rankingType, backfillEvents, new EventStore(appName),
                    duration, offsetDate);
            rankRDDs.add(new Tuple2<>(rankingFieldName, rankRdd));
        }
        JavaPairRDD<String, HashMap<String, JsonAST.JValue>> acc = RDDUtils.getEmptyPairRDD(sc);
        //TODO: is functional fold and map more efficient than looping
        for (Tuple2<String, JavaPairRDD<String, Double>> t : rankRDDs) {
            String fieldName = t._1();
            JavaPairRDD<String, Double> rightRdd = t._2();
            JavaPairRDD<String, Tuple2<org.apache.spark.api.java.Optional<HashMap<String, JsonAST.JValue>>, org.apache.spark.api.java.Optional<Double>>> joined = acc.fullOuterJoin(rightRdd);
            acc = joined.mapToPair(new RankFunction(fieldName));
        }
        return acc;
    }

    private NullModel calcAll(SparkContext sc, PreparedData preparedData) {
        return calcAll(sc, preparedData, true);
    }


    @Override
    public NullModel train(SparkContext sc, PreparedData preparedData) {
        switch (this.recsModel) {
            case RecsModel.All: return this.calcAll(sc, preparedData);
            case RecsModel.BF: return calcPop(sc, preparedData);
            case RecsModel.CF: return this.calcAll(sc, preparedData, false);
            default:
                throw new IllegalArgumentException(
                        String.format("| Bad algorithm param recsModel=[%s] in engine defintion params," +
                        " possibly a bad json value.    |Use one of the available parameter values(%s).", this.recsModel,
                        new RecsModel().toString())
                );
        }
    }

    private NullModel calcPop(SparkContext sc, PreparedData preparedData) {
        JavaPairRDD<String, HashMap<String, JsonAST.JValue>> fieldsRDD = preparedData.getFieldsRDD();
        JavaPairRDD<String, HashMap<String, JsonAST.JValue>> ranksRDD = getRanksRDD(fieldsRDD, sc);

        JavaPairRDD<String, HashMap<String, JsonAST.JValue>> currentMetadata = EsClient
                                                                                .getInstance()
                                                                                .getRDD(esIndex, esType, sc);

        JavaPairRDD<String, HashMap<String, JsonAST.JValue>> propertiesRDD = currentMetadata
                                                                                .fullOuterJoin(ranksRDD)
                                                                                .mapToPair(new CalcAllFunction());
        //Singleton list for propertiesRDD
        ArrayList<JavaPairRDD<String, HashMap<String, JsonAST.JValue>>> pList = new ArrayList<>();
        pList.add(fieldsRDD.cache());
        pList.add(propertiesRDD.cache());
        new JamModel(
                new ArrayList<>(),
                pList,
                getRankingMapping(),
                false,
                sc
                ).save(dateNames, esIndex, esType);
        return new NullModel();
    }


    @Override
    public PredictedResult predict(NullModel model, Query query) {
        return null;
    }

    private Tuple2<List<BoostableCorrelators>, List<Event>> getBiasedRecentUserActions(Query query) {
        List<Event> recentEvents = new ArrayList<>();
        try {
            recentEvents = LJavaEventStore.findByEntity(
                    this.appName,
                    "user",
                    query.getUser(),
                    OptionHelper.<String>none(),
                    Option.apply(queryEventNames),
                    (OptionHelper.none()),
                    (OptionHelper.none()),
                    OptionHelper.none(),
                    OptionHelper.none(),
                    OptionHelper.none(),
                    true,
                    Duration.create(200, "millis")
            );
        } catch (NoSuchElementException nsee) {
            logger.info("No user id for recs, returning similar items for the item specified");
        } catch (Exception e){
            logger.error("Error when read recent events : \n");
            throw e;
        }

        Float userEventBias = query.getUserBiasOrElse(userBias);
        Float userEventsBoost;
        if (userEventBias > 0 && userEventBias != 1) {
            userEventsBoost = userEventBias;
        } else {
            userEventsBoost = null;
        }

        List<BoostableCorrelators> boostableCorrelators = new ArrayList<>();

        for (String action : queryEventNames) {
            Set<String> items = new HashSet<>();

            for (Event e : recentEvents) {
                if (e.event().equals(action) && items.size() < maxQueryEvents && !e.targetEntityId().isEmpty()) {
                    items.add(e.targetEntityId().get()); //converting Option<string> to j's string (event is native scala)
                }
            }
            List<String> stringList = new ArrayList<>(items); // boostable correlators needs a unique list
            boostableCorrelators.add(new BoostableCorrelators(action, stringList, userEventsBoost));
        }
        return new Tuple2<>(boostableCorrelators, recentEvents);
    }

    private List<JsonElement> buildQueryMust(Query query, List<BoostableCorrelators> boostable) {

        List<FilterCorrelators> recentUserHistoryFilter = new ArrayList<>();
        if (userBias < 0.0f) {
            recentUserHistoryFilter = boostable.stream().map(
                    BoostableCorrelators::toFilterCorrelators
            ).collect(Collectors.toList()).subList(0, maxQueryEvents - 1);
        }

        List<FilterCorrelators> similarItemsFilter = new ArrayList<>();
        if (userBias < 0.0f) {
            similarItemsFilter = getBiasedSimilarItems(query).stream()
                    .map(BoostableCorrelators::toFilterCorrelators)
                    .collect(Collectors.toList())
                    .subList(0, maxQueryEvents - 1);
        }
        List<FilterCorrelators> filteringMetadata = getFilterMetadata(query);
        List<JsonElement> filteringDateRange = getFilteringDateRange(query);

        List<FilterCorrelators> allFilteringCorrelators = new ArrayList<>(recentUserHistoryFilter);
        allFilteringCorrelators.addAll(similarItemsFilter);
        allFilteringCorrelators.addAll(filteringMetadata);
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        List<JsonElement> mustFields = allFilteringCorrelators.stream().map(
                filterCorrelator -> {
                    String actionName = filterCorrelator.actionName;
                    List<String> itemIDs = filterCorrelator.itemIDs;

                    JsonObject obj = new JsonObject();
                    JsonObject innerObj = new JsonObject();

                    JsonElement itemIDsObj = gson.toJsonTree(itemIDs, new TypeToken<List<String>>() {

                    }.getType());

                    innerObj.add(actionName, itemIDsObj);
                    innerObj.addProperty("boost", 0);
                    obj.add("terms", innerObj);
                    return obj;
                }
        ).collect(Collectors.toList());
        mustFields.addAll(filteringDateRange);
        return mustFields;
    }

    private List<FilterCorrelators> getFilterMetadata(Query query) {
        List<Field> paramsFilterFields = this.fields.stream()
                                            .filter(field -> field.getBias() >= 0.0f)
                                            .collect(Collectors.toList());
        List<Field> queryFilterFields = query.getFields().stream()
                                            .filter(field -> field.getBias() < 0.0f)
                                            .collect(Collectors.toList());
        paramsFilterFields.addAll(queryFilterFields);
        return paramsFilterFields.stream()
                .map(field -> new FilterCorrelators(field.getName(), field.getValues()))
                .distinct()
                .collect(Collectors.toList());
    }

    private List<JsonElement> getFilteringDateRange(Query query) {
        DateTime currentDate = query.getCurrentDate();
        if (currentDate == null)
            currentDate = DateTime.now().toDateTimeISO();
        String currentdate = currentDate.toString();
        List<JsonElement> json = new ArrayList<>();
        DateRange dr = query.getDateRange();

        if (dr != null && (dr.getAfter() != null || dr.getBefore() != null)) {
            String name = dr.getName();

            DateTime beforeDate = dr.getBefore();
            DateTime afterDate = dr.getAfter();

            String before = beforeDate == null ? "" : beforeDate.toString();
            String after = afterDate ==null ? "" : afterDate.toString();

            StringBuilder rangeStart = new StringBuilder()
                    .append("\n{\n  \"constant_score\": {\n     \"filter\": {\n")
                    .append("      \"range\" : {\n      \"")
                    .append(name)
                    .append("\": {\n");

            String rangeAfter = new StringBuilder()
                    .append("\n         \"lt\" : \"")
                    .append(after)
                    .append("\"\n")
                    .toString();

            String rangeBefore = new StringBuilder()
                    .append("\n         \"gt\": \"")
                    .append(before)
                    .append("\"\n")
                    .toString();

            String rangeEnd = new StringBuilder()
                    .append("\n     }\n     }\n     },\n    \"boost\": 0\n  }\n}\n")
                    .toString();

            if (!after.isEmpty()) {
                    rangeStart.append(rangeAfter);
                    if (!before.isEmpty())
                        rangeStart.append(rangeBefore);
            }

            if (!before.isEmpty())
                rangeStart.append(rangeBefore);

            rangeStart.append(rangeEnd);
            JsonElement el = new JsonParser().parse(rangeStart.toString());
            json.add(el);

        } else if (ap.getAvailableDateName() != null && ap.getExpireDateName() != null) {
            String availableDate = ap.getAvailableDateName();
            String expireDate = ap.getExpireDateName();

            String available = new StringBuilder()
                    .append("\n{\n  \"constant_score\": {\n     \"filter\": {\n")
                    .append("      \"range\" : {\n      \"")
                    .append(availableDate)
                    .append("\":{\n     \"lt\": \"")
                    .append(currentDate)
                    .append("\"\n   }\n     },\n    \"boost\": 0\n  }\n}\n")
                    .toString();

            String expire = new StringBuilder()
                    .append("\n{\n  \"constant_score\": {\n     \"filter\": {\n")
                    .append("      \"range\" : {\n      \"")
                    .append(expireDate)
                    .append("\":{\n     \"gt\": \"")
                    .append(currentDate)
                    .append("\"\n   }\n     },\n    \"boost\": 0\n  }\n}\n")
                    .toString();

            JsonElement avEl = new JsonParser().parse(available);
            JsonElement exEl = new JsonParser().parse(expire);
            json.add(avEl);
            json.add(exEl);
        } else {
            logger.info("\nMisconfigured date information, either your engine.json date settings " +
                    "or your query's dateRange is incorrect.\n" +
                    "Ignoring date information for this query" );
        }

        return json;
    }

    private Tuple2<String, List<Event>> buildQuery(Query query) {
        List<String> backfillFieldsNames = this.rankingFieldNames; // created during it

        try {
            Tuple2<List<BoostableCorrelators>, List<Event>> boostableEvents = getBiasedRecentUserActions(query);
            int numRecs = query.getNumOrElse(limit);
            List<JsonElement> should = buildQueryShould(query, boostableEvents._1());
            List<JsonElement> must = buildQueryMust(query, boostableEvents._1());
            JsonElement mustNot = buildQueryMustNot(query, boostableEvents._2());
            List<JsonElement> sort = buildQuerySort();

            JsonObject jsonQuery = new JsonObject(); //outer most
            JsonObject innerObj = new JsonObject();
            JsonObject innerMostObject = new JsonObject();

            //add "size" to outer query
            jsonQuery.addProperty("size", numRecs);

            //add "query" to outer query
            jsonQuery.add("query", innerObj);
            innerObj.add("bool", innerMostObject);

            JsonArray shouldJsonArray = new JsonArray();
            if (should != null) should.forEach(shouldJsonArray::add);
            innerMostObject.add("should", shouldJsonArray);

            JsonArray mustJsonArray = new JsonArray();
            if (must != null) must.forEach(mustJsonArray::add);
            innerMostObject.add("must", mustJsonArray);

            innerMostObject.add("must_not", mustNot);
            innerMostObject.addProperty("minimum_should_match", 1);

            //add "sort" to outer query
            JsonArray sortJsonArray = new JsonArray();
            if (sort != null) sort.forEach(sortJsonArray::add);
            jsonQuery.add("sort", sortJsonArray);

            String queryStr = jsonQuery.toString();
            logger.info("Query : " + queryStr);

            return new Tuple2<>(queryStr, boostableEvents._2());
        } catch (IllegalArgumentException iae) {
            logger.debug("IllegalArgumentException " + iae.getMessage());
            return new Tuple2<>("", new ArrayList<>());
        }
    }

    private List<BoostableCorrelators> getBiasedSimilarItems(Query query) {
        if (query.getItem() != null) {
            Map<String, Object> m = EsClient.getInstance().getSource(esIndex, esType, query.getItem());
            if (m != null) {
                Float itemEventBias = query.getItemBias() == null ? itemBias : query.getItemBias();
                Float itemEventBoost = (itemEventBias > 0 && itemEventBias != 1) ? itemEventBias : null;

                ArrayList<BoostableCorrelators> out = new ArrayList<>();
                for (String action : modelEventNames) {
                    ArrayList<String> items;
                    try {
                        if (m.containsKey(action) && m.get(action) != null) {
                            items = (ArrayList<String>) m.get(action);
                        } else {
                            items = new ArrayList<>();
                        }
                    } catch (ClassCastException cce) {
                        logger.warn("Bad value in item [$(query.item)] corresponding to key: " +
                                "[$action] that was not a Seq[String] ignored.");
                        items = new ArrayList<>();
                    }
                    List<String> rItems = (items.size() <= maxQueryEvents) ? items : items.subList(0, maxQueryEvents - 1);
                    out.add(new BoostableCorrelators(action, rItems, itemEventBoost));
                }
                return out;
            } else {
                return new ArrayList<>();
            }
        } else {
            return new ArrayList<>();
        }

    }


    private List<BoostableCorrelators> getBoostedMetadata(Query query) {

        ArrayList<Field> paramsBoostedFields = new ArrayList<>();
        for (Field f : fields) {
            if (f.getBias() >= 0f) {
                paramsBoostedFields.add(f);
            }
        }
        ArrayList<Field> queryBoostedFields = new ArrayList<>();
        if (query.getFields() != null) {
            for (Field f : query.getFields()) {
                if (f.getBias() >= 0f) {
                    queryBoostedFields.add(f);
                }
            }
        }
        Set<BoostableCorrelators> out = new HashSet<>();
        for (Field f : queryBoostedFields) {
            out.add(new BoostableCorrelators(f.getName(), f.getValues(), f.getBias()));
        }

        for (Field f : paramsBoostedFields) {
            out.add(new BoostableCorrelators(f.getName(), f.getValues(), f.getBias()));
        }
        return new ArrayList<>(out);
    }


    private List<JsonElement> buildQueryShould(Query query, List<BoostableCorrelators> boostable) {
        //create a list of all boosted query corrleators
        List<BoostableCorrelators> recentUserHistory;
        if (userBias >= 0f) {
            recentUserHistory = boostable.subList(0, Math.min(maxQueryEvents - 1, boostable.size()));
        } else {
            recentUserHistory = new ArrayList<>();
        }
        List<BoostableCorrelators> similarItems;
        if (itemBias >= 0f) {
            similarItems = getBiasedSimilarItems(query);
        } else {
            similarItems = new ArrayList<>();
        }

        List<BoostableCorrelators> boostedMetadata = getBoostedMetadata(query);
        recentUserHistory.addAll(similarItems);
        recentUserHistory.addAll(boostedMetadata);

        ArrayList<JsonElement> shouldFields = new ArrayList<>();
        Gson gson = new Gson();

        for (BoostableCorrelators bc : recentUserHistory) {
            JsonObject obj = new JsonObject();
            JsonObject innerObject = new JsonObject();
            JsonElement jsonElement = gson.toJsonTree(bc.itemIDs, new TypeToken<List<String>>() {

            }.getType());
            innerObject.add(bc.actionName, jsonElement);

            obj.add("terms", innerObject);
            if (bc.boost != null)
                innerObject.addProperty("boost", bc.boost);
            shouldFields.add(obj);
        }

        String shouldScore =
                "{\n" +
                        "   \"constant_score\": {\n" +
                        "   \"filter\": {\n" +
                        "   \"match_all\": {}\n" +
                        "},\n" +
                        "   \"boost\": 0\n" +
                        "   }\n" +
                        "};";
        shouldFields.add(new JsonParser().parse(shouldScore).getAsJsonObject());
        return shouldFields;
    }
    /**
     * build sort query part
     */
    private List<JsonElement> buildQuerySort() {
        if (recsModel.equals(RecsModel.All) || recsModel.equals(RecsModel.BF)) {
            List<JsonElement> sortByScore = new ArrayList<>();
            List<JsonElement> sortByRanks = new ArrayList<>();

            sortByScore.add(new JsonParser().parse("{\"_score\": {\"order\": \"desc\"}}"));
            for (String fieldName : rankingFieldNames) {
                sortByRanks.add(new JsonParser().parse("{\"" + fieldName + "\" :{ \"unmapped_type\": \"double\", \"order\": \"desc\" }}"));
            }
            sortByScore.addAll(sortByRanks);
            return sortByScore;
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * build must not query part
     */
    private JsonElement buildQueryMustNot(Query query, List<Event> events) {
        Gson gson = new Gson();
        JsonObject obj = new JsonObject();

        JsonObject innerObject = new JsonObject();

        List<String> blackList = getExcludeditems(events, query);
        System.out.print(blackList);
        JsonElement vals = gson.toJsonTree(blackList, new TypeToken<List<String>>() {

        }.getType());
        innerObject.add("values", vals);
        innerObject.addProperty("boost", 0);
        innerObject.add("ids", innerObject);

        return obj;
    }

    /**
     * Create a list of item ids that the user has intercated with or are not to be included in recs
     */

    private List<String> getExcludeditems(List<Event> events, Query query) {
        List<Event> blacklistedItems = new ArrayList<>();
        List<String> blacklistedStrings = new ArrayList<>();
        //either a list or an empty list of filtering events so honor them
        logger.info("[getExcludedItems] events: " + modelEventNames.size() + "; blackListEvents: " + blacklistEvents.size()
        + "modelEventNames: " + modelEventNames.size() + ", " + modelEventNames.get(0));

        for (Event event : events) {
            if (blacklistEvents.isEmpty()) {
                if (event.event().equals(modelEventNames.get(0))) {
                    blacklistedItems.add(event);
                }
            } else if (blacklistEvents.contains(event.event())) {
                blacklistedItems.add(event);
            }
        }

        for (Event event : blacklistedItems) {
            if (event.targetEntityId().get() != null) {
                blacklistedStrings.add(event.targetEntityId().get());
            } else {
                blacklistedStrings.add("");
            }
        }

        logger.info("Number of blacklisted items: " + ((Integer) blacklistedItems.size()).toString());
        blacklistedStrings.addAll(query.getBlacklistItems().stream().distinct().collect(Collectors.toList()));

        //now conditionnally add the query item itself
        Boolean includeSelf;
        if (query.getReturnSelf() != null) {
            includeSelf = query.getReturnSelf();
        } else {
            includeSelf = returnSelf;
        }

        if (!includeSelf && (query.getItem()) != null) {
            blacklistedStrings.add(query.getItem());
        }

        List<String> allExcludedStrings = new ArrayList<>();
        allExcludedStrings.addAll(blacklistedStrings.stream().distinct().collect(Collectors.toList()));
        return allExcludedStrings;
    }
}
