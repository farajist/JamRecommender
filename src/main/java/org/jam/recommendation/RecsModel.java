package org.jam.recommendation;


/** Availiable value for algorithm param "RecsModel"
 *
 *
 */
public class RecsModel {

    public static final String All = "all";
    public static final String CF = "collabFiltering";
    public static final String BF = "backfill";

    @Override
    public String toString() {
        return All + " | " + CF + " | " + BF;
    }

}