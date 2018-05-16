package org.jam.recommendation;


import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * now this is a stupid duplicate of the RankingType class
 * todo: keep one the other
 */
public class RankingFieldName {

    public static final String USER_RANK = "userRank";
    public static final String UNIQUE_RANK = "uniqueRank";
    public static final String POP_RANK = "popRank";
    public static final String TREND_RANK = "trenRank";
    public static final String HOT_RANK = "hotRank";
    public static final String UNKNOWN_RANK = "unknownRank";


    public static List<String> toList() {
        List<String> list = new LinkedList<>();
        list.addAll(Arrays.asList(USER_RANK, UNIQUE_RANK, POP_RANK,
                TREND_RANK, HOT_RANK));
        return list;
    }

    @Override
    public String toString() {
        return String.format("%s, %s, %s, %s, %s", USER_RANK, UNIQUE_RANK,
                POP_RANK, TREND_RANK, HOT_RANK);
    }
}