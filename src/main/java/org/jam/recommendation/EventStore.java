package org.jam.recommendation;

import org.apache.predictionio.data.storage.Event;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.Interval;

import java.util.List;

public class EventStore implements IEventStore {

    private final String appName;

    public EventStore(String appName) {
        this.appName = appName;
    }

    @Override
    public JavaRDD<Event> eventsRDD(SparkContext sc, List<String> eventNames, Interval interval) {
        return null;
    }

    @Override
    public JavaRDD<Event> eventsRDD(SparkContext sc, Interval interval) {
        return null;
    }
}
