package org.jam.recommendation;

import grizzled.slf4j.Logger;
import org.apache.predictionio.core.EventWindow;
import org.apache.predictionio.core.SelfCleaningDataSource;
import org.apache.predictionio.core.SelfCleaningDataSource$class;
import org.apache.predictionio.data.storage.*;
import org.slf4j.LoggerFactory;
import org.apache.predictionio.controller.EmptyParams;
import org.apache.predictionio.controller.java.PJavaDataSource;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.predictionio.data.store.java.PJavaEventStore;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterable;
import scala.collection.immutable.Set;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Read specified events from PEventStore and creates RDDs for each event.
 * A list of pairs (eventName, eventRDD) are sent to the preparator for further processing.
 * @author farajist
 * @param dsp params taken from engine.json {@see DataSourceParams}
 * 
 */

public class DataSource extends PJavaDataSource<TrainingData, EmptyParams, Query, Set<String>>
        implements SelfCleaningDataSource {

    private final DataSourceParams dsp;
//    private final EventWindow eventWindow;
    private transient PEvents pEventsDb = Storage.getPEvents();
    private transient LEvents lEventsDb = Storage.getLEvents(false);

    private final Logger logger = new Logger(LoggerFactory.getLogger(SelfCleaningDataSource.class));
    
    public DataSource(DataSourceParams dsp) {
        this.dsp = dsp;

//        drawInfo("Init DataSource", new ArrayList<Tuple2<String, String>>(
//            new Tuple2("══════════════════════════════", "════════════════════════════"),
//            new Tuple2("App name", getAppName()),
//            new Tuple2("Event window", getEventWindow()),
//            new Tuple2("Event names", getEventNames())
//        ));
    }

    public String getAppName() {
        return dsp.getAppName();
    }

    public EventWindow getEventWindow() {
        return dsp.getEventWindow();
    }

    public List<String> getEventNames() {
        return getEventNames();
    }


    @Override
    public TrainingData readTraining(SparkContext sc) {

        ArrayList<String> eventNames = dsp.getEventNames();
        /**
         * collect events associated with particular app name and eventNames from
         * the event store
         **/
        JavaRDD<Event> eventsRDD = PJavaEventStore.find(
            dsp.getAppName(),                             //return event of this app
            OptionHelper.<String>none(),                  //return event of this channel (default)
            OptionHelper.<DateTime>none(),                //return events with eventTime >= this
            OptionHelper.<DateTime>none(),                //return events with eventTime < this
            OptionHelper.some("user"),                    //return events of this entity type
            OptionHelper.<String>none(),                  //return events of this entity id 
            OptionHelper.some(dsp.getEventNames()),       //return events with any any of this event names
            OptionHelper.some(OptionHelper.some("item")), //return events on this target type
            OptionHelper.<Option<String>>none(),          //return event on this id  (none == no restriction)
            sc
        );

        //separate events by event name, now every event name maps to an action rdd 
        List<Tuple2<String, JavaPairRDD<String, String>>> actionRDDs = 
            eventNames.stream()
                    .map(eventName -> {
                        JavaRDD<Tuple2<String, String>> actionRDD = 
                        eventsRDD.filter(event -> !event.entityId().isEmpty()
                                        && !event.targetEntityId().get().isEmpty()
                                        && eventName.equals(event.event()))
                                        .map(event -> new Tuple2<String, String>(
                                            event.entityId(),
                                            event.targetEntityId().get()));
                        //NOTE: here we have it like (event => <user, item>) with all entites known
                        return new Tuple2<>(eventName, JavaPairRDD.fromJavaRDD(actionRDD));
                    })
                    .filter(pair -> !pair._2().isEmpty())
                    .collect(Collectors.toList());

        //aggregate all $set/$unset s for metadata fields, which are attached to items
        JavaRDD<Tuple2<String, PropertyMap>> fieldsRDD = 
            PJavaEventStore.aggregateProperties(
                dsp.getAppName(),                           //app name
                "item",                                     //entity type
                OptionHelper.<String>none(),                //channel
                OptionHelper.<DateTime>none(),              // start time
                OptionHelper.<DateTime>none(),              // end time
                OptionHelper.<List<String>>none(),          //required entities
                sc    
            ).repartition(sc.defaultParallelism());
        
        //convert to a pair string (item?)/propertyMap (metadata?)
        JavaPairRDD<String, PropertyMap> fieldsJavaPairRDD = JavaPairRDD.fromJavaRDD(fieldsRDD);
        return new TrainingData(actionRDDs, fieldsJavaPairRDD);
    }


    private boolean isSetEvent(Event e) {
        return e.event().equals("$set") || e.event().equals("$unset");
    }

    public Logger logger() {
        return this.logger;
    }

    @Override
    public String appName() {
        return dsp.getAppName();
    }

    @Override
    public Option<EventWindow> eventWindow() {
        return SelfCleaningDataSource$class.eventWindow(this);
    }

    @Override
    public RDD<Event> getCleanedPEvents(RDD<Event> pEvents) {
        return SelfCleaningDataSource$class.getCleanedPEvents(this, pEvents);
    }

    @Override
    public Iterable<Event> getCleanedLEvents(Iterable<Event> lEvents) {
        return SelfCleaningDataSource$class.getCleanedLEvents(this, lEvents);
    }

    @Override
    public Event recreateEvent(Event x, Option<String> eventId, DateTime creationTime) {
        return SelfCleaningDataSource$class.recreateEvent(this, x, eventId, creationTime);
    }

    @Override
    public Iterable<Event> removeLDuplicates(Iterable<Event> ls) {
        return SelfCleaningDataSource$class.removeLDuplicates(this, ls);
    }

    @Override
    public void removeEvents(Set<String> eventsToRemove, int appId) {
        SelfCleaningDataSource$class.removeEvents(this, eventsToRemove, appId);
    }

    @Override
    public RDD<Event> compressPProperties(SparkContext sc, RDD<Event> rdd) {
        return SelfCleaningDataSource$class.compressPProperties(this, sc, rdd);
    }

    @Override
    public RDD<Event> removePDuplicates(SparkContext sc, RDD<Event> rdd) {
        return SelfCleaningDataSource$class.removePDuplicates(this, sc, rdd);
    }

    @Override
    public Iterable<Event> compressLProperties(Iterable<Event> events) {
        return SelfCleaningDataSource$class.compressLProperties(this, events);
    }

    @Override
    public void cleanPersistedPEvents(SparkContext sc) {
        SelfCleaningDataSource$class.cleanPersistedPEvents(this, sc);
    }

    @Override
    public void wipePEvents(RDD<Event> newEvents, RDD<String> eventsToRemove, SparkContext sc) {
        SelfCleaningDataSource$class.wipePEvents(this, newEvents, eventsToRemove, sc);
    }

    @Override
    public void removePEvents(RDD<String> eventsToRemove, int appId, SparkContext sc) {
        SelfCleaningDataSource$class.removePEvents(this, eventsToRemove, appId, sc);
    }
    @Override
    public void wipe(Set<Event> newEvents, Set<String> eventsToRemove) {
        SelfCleaningDataSource$class.wipe(this, newEvents, eventsToRemove);
    }

    @Override
    public RDD<Event> cleanPEvents(SparkContext sc) {
        return SelfCleaningDataSource$class.cleanPEvents(this, sc);
    }

    @Override
    public void cleanPersistedLEvents() {
        SelfCleaningDataSource$class.cleanPersistedLEvents(this);
    }

    @Override
    public scala.collection.Iterable<Event> cleanLEvents() {
        return SelfCleaningDataSource$class.cleanLEvents(this);
    }

    public LEvents org$apache$predictionio$core$SelfCleaningDataSource$$lEventsDb() {
        return this.lEventsDb;
    }

    public PEvents org$apache$predictionio$core$SelfCleaningDataSource$$pEventsDb() {
        return this.pEventsDb;
    }

    public org.apache.predictionio.core.SelfCleaningDataSource.DateTimeOrdering$ DateTimeOrdering() {
        //TODO
        return null;
    }
}