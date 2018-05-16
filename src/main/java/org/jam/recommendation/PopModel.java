package org.jam.recommendation;


import org.apache.predictionio.data.storage.Event;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PopModel {

    private transient static final Logger logger = LoggerFactory.getLogger(PopModel.class);
    public static final Map<String, String> nameByType;

    static {
        //todo: scala version has  default value of RankingFieldName. Do something similar ?

        Map<String, String> map = new HashMap<>();
        map.put(RankingType.POPULAR, RankingFieldName.POP_RANK);
        map.put(RankingType.TRENDING, RankingFieldName.TREND_RANK);
        map.put(RankingType.HOT, RankingFieldName.HOT_RANK);
        map.put(RankingType.USER_DEFINED, RankingFieldName.USER_RANK);
        map.put(RankingType.RANDOM, RankingFieldName.UNIQUE_RANK);
        nameByType = Collections.unmodifiableMap(map);
    }

    private final JavaPairRDD<String, HashMap<String, JsonAST.JValue>> fieldsRDD; //itemID -> itemProps
    private final SparkContext sc;

    public PopModel(JavaPairRDD<String, HashMap<String, JsonAST.JValue>> fieldsRDD, SparkContext sc) {
        this.fieldsRDD = fieldsRDD;
        this.sc = sc;
    }

    /**
     * create random rank for all items
     * @param modelName name of model
     * @param
     */

    public JavaPairRDD<String, Double> calc(String modelName, List<String> eventNames,
                                            IEventStore eventStore, Integer duration, String offsetDate) {

        DateTime end;
        if (offsetDate == null) {
            end = DateTime.now();
        } else {
            try {
                end = ISODateTimeFormat.dateTimeParser().parseDateTime(offsetDate);
            } catch (IllegalArgumentException iae) {
                logger.warn("Bad end for popModel : " + offsetDate + " using 'now()'");
                end = DateTime.now();
            }
        }

        final Interval interval = new Interval(end.minusSeconds(duration), end);

        //based on typeof populatiry model a set of (item-id, ranking-number) for all items
        logger.info("PopModel " + modelName + " using end: " + end + " , and duration: " + duration
                + " , interval: " + interval);

        switch(modelName) {
            case RankingType.POPULAR :
                return calcPopular(eventStore, eventNames, interval);
            case RankingType.TRENDING :
                return calcTrending(eventStore, eventNames, interval);
            case RankingType.HOT :
                return calcHot(eventStore, eventNames, interval);
            case RankingType.RANDOM :
                return calcRandom(eventStore, interval);
            case RankingType.USER_DEFINED :
                return RDDUtils.getEmptyPairRDD(sc);
            default:
                logger.warn("\n\tBad ranking params type=[$unknownRankingType] in engine definition params, " +
                        "possibly a bad json value. \n\tUse one of the available parameter values ($RankingType).");
                return RDDUtils.getEmptyPairRDD(sc);
        }

    }

    /**
     *
     * @param eventStore
     * @param eventNames
     * @param interval
     * @return
     */

    public JavaPairRDD<String,Double> calcPopular(IEventStore eventStore, List<String> eventNames, Interval interval) {
        logger.info("PopModel getting eventsRDD for startTime: " + interval.getStart() +
                " and endTime " + interval.getEnd());
        final JavaRDD<Event> events = eventStore.eventsRDD(sc, eventNames, interval);
        return events.mapToPair(e -> new Tuple2<String, Integer>(e.targetEntityId().get(), 1))
                .reduceByKey((a,b) -> a + b)
                .mapToPair(t -> new Tuple2<>(t._1(), (double) t._2()));
    }

    /**
     *
     * @param eventStore
     * @param eventNames
     * @param interval
     * @return
     */
    public JavaPairRDD<String,Double> calcTrending(IEventStore eventStore, List<String> eventNames, Interval interval) {
        logger.info("Current Interval: " + interval + ", " +interval.toDurationMillis());
        final long halfInterval = interval.toDurationMillis() / 2;
        final Interval olderInterval = new Interval(interval.getStart(), interval.getStart().plus(halfInterval));
        logger.info("Older Interval: " + olderInterval);
        final Interval newerInterval = new Interval(interval.getStart().plus(halfInterval), interval.getEnd());
        logger.info("Newer Interval: " + newerInterval);

        final JavaPairRDD<String, Double> olderPopRDD = calcPopular(eventStore, eventNames, newerInterval);
        if (!olderPopRDD.isEmpty()) {
            final JavaPairRDD<String, Double> newerPopRDD = calcPopular(eventStore, eventNames, olderInterval);
            return newerPopRDD.join(olderPopRDD)
                    .mapToPair(t -> new Tuple2<String, Double>(t._1, t._2._1 - t._2._2));
        } else {
            return RDDUtils .getEmptyPairRDD(sc);
        }
    }

    /**
     *
     * @param eventStore
     * @param eventNames
     * @param interval
     * @return
     */
    private JavaPairRDD<String,Double> calcHot(IEventStore eventStore, List<String> eventNames, Interval interval) {
        logger.info("Current Interval: " + interval + ", " +interval.toDurationMillis());
        final Interval olderInterval = new Interval(interval.getStart(), interval.getStart().plus(interval.toDurationMillis() / 3));
        logger.info("Older Interval: " + olderInterval);
        final Interval middleInterval = new Interval(olderInterval.getEnd(), olderInterval.getEnd().plus(olderInterval.toDurationMillis()));
        logger.info("Middle interval: " + middleInterval);
        final Interval newerInterval = new Interval(middleInterval.getEnd(), interval.getEnd());
        logger.info("Newer Interval: " + newerInterval);

        final JavaPairRDD<String, Double> olderPropRDD = calcPopular(eventStore, eventNames, olderInterval);
        if (!olderPropRDD.isEmpty()) {
            final JavaPairRDD<String, Double> middlePopRDD = calcPopular(eventStore, eventNames, middleInterval);
            if (!middlePopRDD.isEmpty()) {
                final JavaPairRDD<String, Double> newerPopRDD = calcPopular(eventStore, eventNames, newerInterval);

                final JavaPairRDD<String, Double> newerVelocity = newerPopRDD.join(middlePopRDD)
                                                                            .mapToPair(t -> new Tuple2<>(t._1, t._2._1 - t._2._2));
                final JavaPairRDD<String, Double> olderVelocity = middlePopRDD.join(olderPropRDD)
                                                                            .mapToPair(t -> new Tuple2<>(t._1, t._2._1 - t._2._2));
                return newerVelocity.join(olderVelocity)
                                    .mapToPair(t -> new Tuple2<String, Double>(t._1, t._2._1 - t._2._2));
            } else {
                return RDDUtils.getEmptyPairRDD(sc);
            }
        } else {
            return RDDUtils.getEmptyPairRDD(sc);
        }
    }

    /**
     * creates random rank for all items
     * @param eventStore
     * @param interval
     * @return
     */
    private JavaPairRDD<String,Double> calcRandom(IEventStore eventStore, Interval interval) {
        final JavaRDD<Event> events = eventStore.eventsRDD(sc, interval);
        final JavaRDD<String> actionsRDD = events.map(Event::targetEntityId)
                                                .filter(Option::isDefined)
                                                .map(Option::get)
                                                .distinct();
        final JavaRDD<String> itemsRDD = fieldsRDD.map(Tuple2::_1);
        return actionsRDD.union(itemsRDD).distinct()
                .mapToPair(itemID -> new Tuple2<>(itemID, Math.random()));
    }
}