package org.jam.recommendation;


import org.apache.predictionio.controller.Params;

/**
 * todo: comment and explain each single line
 *
 *
 */
public class IndicatorParams implements Params {

    private final String name;
    private final Integer maxItemsPerUser;
    private final Integer maxCorrelatorsPerItem;
    private final Double minLLR;

    public IndicatorParams(String name, Integer maxItemsPerUser,
                           Integer maxCorrelatorsPerItem, Double minLLR) {
        this.name = name;
        this.maxItemsPerUser = maxItemsPerUser;
        this.maxCorrelatorsPerItem = maxCorrelatorsPerItem;
        this.minLLR = minLLR;
    }

    public String getName() {
        return name;
    }

    public Integer getMaxItemsPerUser() {
        return maxItemsPerUser;
    }

    public Integer getMaxCorrelatorsPerItem() {
        return maxCorrelatorsPerItem;
    }

    public Double getMinLLR() {
        return minLLR;
    }

    @Override
    public String toString() {
        return "IndicatorParams{" +
                "name='" + name + '\'' +
                ", maxItemsPerUser=" + maxItemsPerUser +
                ", maxCorrelatorsPerItem=" + maxCorrelatorsPerItem +
                ", minLLR=" + minLLR +
                '}';
    }
}