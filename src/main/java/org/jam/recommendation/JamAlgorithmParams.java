package org.jam.recommendation;

import org.apache.predictionio.controller.Params;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


/**
 * @author farajist
 * todo: comment use of each and every param, and mention lines of
 * usages in other context, link descripition to default params class
 *
 */
public class JamAlgorithmParams implements Params{

    private final String appName;
    private final String indexName;
    private final String typeName;
    private final String recsModel;

    private final List<String> eventNames;
    private final List<String> blacklistEvents;

    private final Integer maxQueryEvents;
    private final Integer maxEventsPerEventType;
    private final Integer maxCorrelatorsPerEventType;
    private final Integer num;

    private final Float userBias;
    private final Float itemBias;

    private final Boolean returnSelf;

    private final List<Field> fields;
    private final List<RankingParams> rankings;

    private final String availableDateName;
    private final String expireDateName;
    private final String dateName;

    private final List<IndicatorParams> indicators;

    private final Long seed;

    public JamAlgorithmParams(String appName, String indexName, String typeName, String recsModel,
                              List<String> eventNames, List<String> blacklistEvents, Integer maxQueryEvents,
                              Integer maxEventsPerEventType, Integer maxCorrelatorsPerEventType, Integer num,
                              Float userBias, Float itemBias, Boolean returnSelf, List<Field> fields,
                              List<RankingParams> rankings, String availableDateName, String expireDateName,
                              String dateName, List<IndicatorParams> indicators, Long seed) {

        if (appName == null || appName.isEmpty())
            throw new IllegalArgumentException("App name is missing");
        if (indexName == null || indexName.isEmpty())
            throw new IllegalArgumentException("Index name is missing");
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException("Type name is missing");
        if ((eventNames == null || eventNames.isEmpty()) && (indicators == null || indicators.isEmpty()))
            throw new IllegalArgumentException("One of the event names or indicator names can be omitted but not both");

        this.appName = appName;
        this.indexName = indexName;
        this.typeName = typeName;
        this.recsModel = recsModel;
        this.eventNames = eventNames;
        this.blacklistEvents = blacklistEvents;
        this.maxQueryEvents = maxQueryEvents;
        this.maxEventsPerEventType = maxEventsPerEventType;
        this.maxCorrelatorsPerEventType = maxCorrelatorsPerEventType;
        this.num = num;
        this.userBias = userBias;
        this.itemBias = itemBias;
        this.returnSelf = returnSelf;
        this.fields = fields;
        this.rankings = rankings;
        this.availableDateName = availableDateName;
        this.expireDateName = expireDateName;
        this.dateName = dateName;
        this.indicators = indicators;
        this.seed = seed;
    }

    public String getAppName() {
        return appName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getRecsModel() {
        return recsModel;
    }

//    public List<String> getEventNames() {
//        return eventNames;
//    }

    //public List<String> getBlacklistEvents() {
      //  return blacklistEvents;
    //}

    public Integer getMaxQueryEvents() {
        return maxQueryEvents;
    }

    public Integer getMaxEventsPerEventType() {
        return maxEventsPerEventType;
    }

    public Integer getMaxCorrelatorsPerEventType() {
        return maxCorrelatorsPerEventType;
    }

    public Integer getNum() {
        return num;
    }

    public Float getUserBias() {
        return userBias;
    }

    public Float getItemBias() {
        return itemBias;
    }

    public Boolean getReturnSelf() {
        return returnSelf;
    }

//    public List<Field> getFields() {
//        return fields;
//    }

//    public List<RankingParams> getRankings() {
//        return rankings;
//    }

    public String getAvailableDateName() {
        return availableDateName;
    }

    public String getExpireDateName() {
        return expireDateName;
    }

    public String getDateName() {
        return dateName;
    }

//    public List<IndicatorParams> getIndicators() {
//        return indicators;
//    }

    public Long getSeed() {
        return seed;
    }

    protected String getRecsModelOrElse(String defaultValue) {
        return this.recsModel == null || this.recsModel.isEmpty()
                ? defaultValue
                : this.getRecsModel();
    }

    public List<String> getEventNames() {
        return this.eventNames == null
                ? Collections.emptyList()
                : this.eventNames;
    }

    protected List<String> getBlacklistEvents() {
        return this.blacklistEvents == null
                ? Collections.emptyList()
                : this.blacklistEvents;
    }

    protected Integer getMaxQueryEventsOrElse(Integer defaultValue) {
        return this.maxEventsPerEventType == null
                ? defaultValue
                : this.getMaxEventsPerEventType();
    }

    protected Integer getMaxCorrelatorsPerEventTypeOrElse(Integer defaultValue) {
        return this.maxCorrelatorsPerEventType == null
                ? defaultValue
                : this.getMaxCorrelatorsPerEventType();
    }

    protected Integer getNumOrElse(Integer defaultValue) {
        return this.num == null ? defaultValue : this.getNum();
    }

    protected Float getUserBiasOrElse(Float defaultValue) {
        return this.userBias == null
                ? defaultValue
                : this.getUserBias();
    }

    protected Float getItemBiasOrElse(Float defaultValue) {
        return this.itemBias == null
                ? defaultValue
                : this.getItemBias();
    }

    protected Boolean getReturnSelfOrElse(Boolean defaultValue) {
        return this.returnSelf == null
                ? defaultValue
                : this.getReturnSelf();
    }

    protected List<Field> getFields() {
        return this.fields == null
                ? Collections.emptyList()
                : this.fields;
    }

    protected List<RankingParams> getRankingsOrElse(List<RankingParams> defaultValue) {
        return this.rankings == null
                ? defaultValue
                : this.getRankings();
    }

    public List<RankingParams> getRankings() {
        return this.rankings == null
                ? Collections.emptyList()
                : this.rankings;
    }

    protected List<String> getModelEventNames() {
        boolean indicatorIsEmpty = this.getIndicators().isEmpty();
        if (indicatorIsEmpty && this.getEventNames().isEmpty()) {
            throw new IllegalArgumentException("No eventNames or indicators in engine.json" +
                    " and one of these is required");
        } else if (indicatorIsEmpty) {
            return this.getEventNames();
        } else {
            return this.getIndicators().stream()
                    .map(IndicatorParams::getName)
                    .collect(Collectors.toList());
        }
    }

    protected List<IndicatorParams> getIndicators() {
        return this.indicators == null
                ? Collections.emptyList()
                : this.indicators;
    }

    protected Long getSeedOrElse(Long defaultValue) {
        return this.seed == null
                ? defaultValue
                : this.getSeed();
    }

    @Override
    public String toString() {
        return "JamAlgorithmParams{" +
                "appName='" + appName + '\'' +
                ", indexName='" + indexName + '\'' +
                ", typeName='" + typeName + '\'' +
                ", recsModel='" + recsModel + '\'' +
                ", eventNames=" + eventNames +
                ", blacklistEvents=" + blacklistEvents +
                ", maxQueryEvents=" + maxQueryEvents +
                ", maxEventsPerEventType=" + maxEventsPerEventType +
                ", maxCorrelatorsPerEventType=" + maxCorrelatorsPerEventType +
                ", num=" + num +
                ", userBias=" + userBias +
                ", itemBias=" + itemBias +
                ", returnSelf=" + returnSelf +
                ", fields=" + fields +
                ", rankings=" + rankings +
                ", availableDateName='" + availableDateName + '\'' +
                ", expireDateName='" + expireDateName + '\'' +
                ", dateName='" + dateName + '\'' +
                ", indicators=" + indicators +
                ", seed=" + seed +
                '}';
    }

    public Integer getMaxEventsPerEventTypeOrElse(int defaultValue) {
        return this.maxEventsPerEventType == null
                ? defaultValue
                : this.getMaxEventsPerEventType();
    }
}
