package org.jam.recommendation;

import java.io.Serializable;
import java.util.Map;

/**
 * todo : explain use and why ranks are added on top
 * of default recommender implementation
 */

public class ItemScore implements Serializable, Comparable<ItemScore> {
    private final String itemEntityId;
    private final double score;
    private final Map<String, Double> ranks;

    public ItemScore(String itemEntityId, double score, Map<String, Double> ranks) {
        this.itemEntityId = itemEntityId;
        this.score = score;
        this.ranks = ranks;
    }

    public String getItemEntityId() {
        return itemEntityId;
    }

    public double getScore() {
        return score;
    }

    public Map<String, Double> getRanks() {
        return ranks;
    }

    @Override
    public String toString() {
        return "ItemScore{" +
                "itemEntityId='" + itemEntityId + '\'' +
                ", score=" + score +
                ", ranks=" + ranks +
                '}';
    }

    @Override
    public int compareTo(ItemScore o) {
        return Double.valueOf(score).compareTo(o.score);
    }
}
