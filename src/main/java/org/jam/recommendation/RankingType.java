package org.jam.recommendation;


import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class RankingType {

    public static final String POPULAR = "popular";
    public static final String HOT = "hot";
    public static final String TRENDING = "trending";
    public static final String USER_DEFINED = "userDefined";
    public static final String RANDOM = "random";


    public static List<String> toList() {
        List<String> list = new LinkedList<>();
        list.addAll(Arrays.asList(POPULAR, HOT, TRENDING,
                USER_DEFINED, RANDOM));
        return list;
    }

    @Override
    public String toString() {
        return String.format("%s, %s, %s, %s, %s", POPULAR, HOT, TRENDING,
                USER_DEFINED, RANDOM);
    }
}