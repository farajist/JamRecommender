package org.jam.recommendation;


import org.apache.predictionio.controller.Params;

import java.util.Collections;
import java.util.List;

/**
 * @author farajist
 *
 * @param eventNames empty list means using the algo eventNames list,
 *                   otherwise a list of events
 *
 * @param offsetDate used only for tests, specifies the offset date to
 *                   start the duration so the most recent day for events
 *                    going back by from the more recent offsetDate - duration
 * @param duration amount of time worth of events to use in calculation of
 *                 backfill
 */

public class RankingParams implements Params {

    private final String name;
    private final String backfillType;
    private final List<String> eventNames;

    private final String offsetDate;


    private final String endDate;
    private final String duration;


    public RankingParams(String name, String backfillType, List<String> eventNames,
                         String offsetDate, String endDate, String duration) {
        this.name = name;
        this.backfillType = backfillType;
        this.eventNames = eventNames;
        this.offsetDate = offsetDate;
        this.endDate = endDate;
        this.duration = duration;
    }

    public RankingParams() {
        this(null, null, null,
                null, null, null);
    }


    public String getName() {
        return name;
    }

    public String getBackfillType() {
        return backfillType;
    }

    public List<String> getEventNames() {
        if (this.eventNames == null) {
            return Collections.<String>emptyList();
        } else {
            return this.eventNames;
        }
    }

    public String getOffsetDate() {
        return offsetDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public String getDuration() {
        return duration;
    }

    public String getNameOrElse(String defaultValue) {
        return this.name == null
                ? defaultValue
                : this.getName();

    }

    public String getBackfillTypeOrElse(String defaultValue) {
        return this.backfillType == null
                ? defaultValue
                : this.getBackfillType();
    }

    @Override
    public String toString() {
        return "RankingParams{" +
                "name='" + name + '\'' +
                ", backfillType='" + backfillType + '\'' +
                ", eventNames=" + eventNames +
                ", offsetDate='" + offsetDate + '\'' +
                ", endDate='" + endDate + '\'' +
                ", duration='" + duration + '\'' +
                '}';
    }
}