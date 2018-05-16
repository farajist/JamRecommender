package org.jam.recommendation;

import com.google.gson.TypeAdapterFactory;
import org.apache.predictionio.controller.CustomQuerySerializer;
import org.joda.time.DateTime;
import org.json4s.Formats;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.*;

/** represents a query in JR, has all necessary fields
 * @author farajist
 * self-explanatory but add comments _-
 */
public class Query implements Serializable, CustomQuerySerializer {
    private final String user;
    private final Float userBias;
    private final String  item;
    private final Float itemBias;
    private final List<Field> fields;
    private final DateTime currentDate;
    private final DateRange dateRange;
    private final List<String> blacklistItems;
    private final Boolean returnSelf;
    private final Integer num;
    private final List<String> eventNames;
    private final Boolean withRanks;

    public Query(String user, Float userBias, String item, Float itemBias, List<Field> fields,
                 DateTime currentDate, DateRange dateRange, List<String> blacklistItems,
                 Boolean returnSelf, Integer num, List<String> eventNames, Boolean withRanks) {
        this.user = user;
        this.userBias = userBias;
        this.item = item;
        this.itemBias = itemBias;
        this.fields = fields;
        this.currentDate = currentDate;
        this.dateRange = dateRange;
        this.blacklistItems = blacklistItems;
        this.returnSelf = returnSelf;
        this.num = num;
        this.eventNames = eventNames;
        this.withRanks = withRanks;
    }

    public String getUser() {
        return user;
    }

    public Float getUserBias() {
        return userBias;
    }

    public String getItem() {
        return item;
    }

    public Float getItemBias() {
        return itemBias;
    }

    public List<Field> getFields() {
        return fields;
    }

    public DateTime getCurrentDate() {
        return currentDate;
    }

    public DateRange getDateRange() {
        return dateRange;
    }

    public List<String> getBlacklistItems() {
        if (this.blacklistItems == null) {
            return Collections.<String>emptyList();
        } else {
            return this.blacklistItems;
        }
    }

    public Boolean getReturnSelf() {
        return returnSelf;
    }

    public Integer getNum() {
        return num;
    }

    public List<String> getEventNames() {
        return eventNames;
    }

    public Boolean getWithRanks() {
        return withRanks;
    }

    public Boolean getRankingsOrElse(Boolean defaultValue) {
        return this.withRanks == null
                ? defaultValue
                : this.getWithRanks();
    }

    public Integer getNumOrElse(Integer defaultValue) {
        return this.num == null
                ? defaultValue
                : this.getNum();
    }

    public List<String> getEventNamesOrElse(List<String> defaultValue) {
        return this.eventNames == null
                ? defaultValue
                : this.getEventNames();
    }

    public Float getUserBiasOrElse(Float defaultValue) {
        return this.userBias == null
                ? defaultValue
                : this.getUserBias();
    }

    @Override
    public Formats querySerializer() {
        return null;
    }

    @Override
    public String toString() {
        return "Query{" +
                "user='" + user + '\'' +
                ", userBias=" + userBias +
                ", item='" + item + '\'' +
                ", itemBias=" + itemBias +
                ", fields=" + fields +
                ", currentDate=" + currentDate +
                ", dateRange=" + dateRange +
                ", blacklistItems=" + blacklistItems +
                ", returnSelf=" + returnSelf +
                ", num=" + num +
                ", eventNames=" + eventNames +
                ", withRanks=" + withRanks +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof Query) {
            Query query = (Query) o;
            return query.getUser().equals(this.getUser())
                    && query.getUserBias().equals(this.getUserBias())
                    && query.getFields().equals(this.getFields())
                    && query.getCurrentDate().equals(this.getCurrentDate())
                    && query.getDateRange().equals(this.getDateRange())
                    && query.getBlacklistItems().equals(this.getBlacklistItems())
                    && query.getReturnSelf().equals(this.getReturnSelf())
                    && query.getNum().equals(this.getNum())
                    && query.getEventNames().equals(this.getEventNames())
                    && query.getWithRanks().equals(this.getWithRanks());
        } else {
            return false;
        }
    }

    @Override
    public Seq<TypeAdapterFactory> gsonTypeAdapterFactories() {
        List<TypeAdapterFactory> typeAdapterFactoryList = new LinkedList<>();
        typeAdapterFactoryList.add(new DateRangeTypeAdapterFactory());
        return JavaConversions.asScalaBuffer(typeAdapterFactoryList).toSeq();
    }
}
