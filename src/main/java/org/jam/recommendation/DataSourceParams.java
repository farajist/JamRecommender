package org.jam.recommendation;

import org.apache.predictionio.core.EventWindow;
import org.apache.predictionio.controller.Params;


/** Taken from engine.json these are passed in the DataSource constructor
 * @param appName registered name for the app
 * @param eventNames a list of events expected. The first is primary
 * event, the rest are secondary. These will be used to create a 
 * primary correlator and cross-occurrence secondary correlators
 * @param eventWindow used for sessionization and events clean ups
 * @param minEventsPerUser defaults to 1 event, if user has only one 
 * then they will not contribute to the training anyway
 */

public class DataSourceParams implements Params{
    private final String appName;
    private final List<String> eventNames;
    private final EventWindow eventWindow;

    public DataSourceParams(String appName, List<String> eventNames, EventWindow eventWindow) {
        this.appName = appName;
        this.eventNames = eventNames;
        this.eventWindow = eventWindow;
    }

    public String getAppName() {
        return this.appName;
    }

    public List<String> getEventNames() {
        return this.appName;
    }

    public EventWindow getEventWindow() {
        return this.eventWindow;
    }
}
