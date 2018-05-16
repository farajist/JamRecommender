package org.jam.recommendation;

import org.apache.predictionio.data.storage.Event;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.Interval;

import java.util.List;

/** EventStore with support for events retrieval within an
 * interval
 */
public interface IEventStore {


    /**
     * retrieve events specified with eventNames within provided interval
     * @param sc used SparkContext
     * @param eventNames names of events we want to look at, retrieve (all if null)
     * @param interval interval of time [interval.start, interval.end]
     * @return JavaRDD &lt Event &gt a set of events
     */
    JavaRDD<Event> eventsRDD(SparkContext sc, List<String> eventNames,
                                    Interval interval);

    /**
     * retrieve all events within provided interval
     * @param sc used SparkContext
     * @param interval interval of time [interval.start, interval.end]
     * @return JavaRDD &lt Event &gt a set of events
     */
    JavaRDD<Event> eventsRDD(SparkContext sc, Interval interval);
}
