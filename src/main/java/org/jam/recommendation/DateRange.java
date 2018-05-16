package org.jam.recommendation;

import com.google.gson.TypeAdapterFactory;
import org.apache.predictionio.controller.CustomQuerySerializer;
import org.joda.time.DateTime;
import org.json4s.Formats;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class DateRange implements Serializable, CustomQuerySerializer {

    private final String name;
    private final String before;
    private final String after;


    public DateRange(String name, String before, String after) {
      if (((before == null) || before.isEmpty()) && (after == null || after.isEmpty())) {
          throw new IllegalArgumentException("one of the bounds can be omitted but not both");
      }

      this.name = name;

      DateTime doomed;
      if (before != null && !before.isEmpty()) {
          doomed = new DateTime(before);
          this.before = before;
      } else {
          this.before = null;
      }

      if (after != null  && !after.isEmpty()) {
          doomed = new DateTime(after);
          this.after = after;
      } else {
          this.after = null;
      }
    }

    public DateTime getBefore() {
        return new DateTime(before);
    }

    public DateTime getAfter() {
        return new DateTime(after);
    }

    public String getName() {
        return name;
    }

    @Override
    public Formats querySerializer() {
        return null;
    }

    @Override
    public Seq<TypeAdapterFactory> gsonTypeAdapterFactories() {
        List<TypeAdapterFactory> typeAdapterFactoryList = new LinkedList<>();
        typeAdapterFactoryList.add(new DateRangeTypeAdapterFactory());
        return JavaConversions.asScalaBuffer(typeAdapterFactoryList).toSeq();
    }

    @Override
    public String toString() {
        return "DateRange{" +
                "name='" + name + '\'' +
                ", before='" + before + '\'' +
                ", after='" + after + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DateRange) {
            DateRange dateRange = (DateRange) o;
            return this.name.equals(dateRange.name)
                    && this.before.equals(dateRange.before)
                    && this.after.equals(dateRange.after);
        } else {
            return false;
        }
    }
}
