package org.jam.recommendation;


/** Setting an option here in the params class doesn't work as
 * expected when the param is missing from engine.json so set these
 * for use in the algorithm when they are not in the engine.json
 *
 * @author farajist
 *
 * @param DEFAULT_MAX_QUERY_EVENTS default number of user history events to use
 *                                 in recs model
 * @param DEFAULT_AVAILABLE_DATE_NAME default name for and item's available after date
 * @param DEFAULT_DATE_NAME when using a date range in the query this is the name of the
 *                          item's date
 * @param DEFAULT_RECS_MODEL use CF + Backfill
 * @param DEFAULT_BACKFILL_DURATION for all time
 *
 * todo: show exact usage of each value
 */
public class DefaultJamAlgorithmParams {

    public static final int DEFAULT_MAX_EVENTS_PER_EVENT_TYPE = 500;
    public static final int DEFAULT_NUM = 10;
    public static final int DEFAULT_MAX_CORRELATORS_PER_EVENT_TYPE = 50;

    public static final int DEFAULT_MAX_QUERY_EVENTS = 100;

    public static final String DEFAULT_AVAILABLE_DATE_NAME = "availableDate";

    public static final String DEFAULT_DATE_NAME = "date";

    public static final String DEFAULT_RECS_MODEL = RecsModel.All;

    public static final RankingParams DEFAULT_RANKING_PARAMS = new RankingParams();
    public static final String DEFAULT_BACKFILL_FIELD_NAME = RankingFieldName.POP_RANK; //why would i need both is it a question of structure ?

    public static final String DEFAULT_BACKFILL_TYPE = RankingType.POPULAR;

    public static String DEFAULT_BACKFILL_DURATION = "3650 days";

    public static Boolean DEFAULT_RETURN_SELF = false;

}