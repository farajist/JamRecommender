package org.jam.recommendation;

import jdk.nashorn.internal.objects.annotations.Property;
import org.apache.predictionio.data.storage.PropertyMap;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.json4s.JsonAST;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RDDUtils {


    public static <T> JavaRDD<T> getEmptyRDD(SparkContext sc) {
        final JavaSparkContext jsc = new JavaSparkContext(sc);
        return jsc.emptyRDD();
    }

    public static <K, V> JavaPairRDD<K, V> getEmptyPairRDD(SparkContext sc) {
        final JavaSparkContext jsc = new JavaSparkContext(sc);
        final JavaRDD<Tuple2<K, V>> empty = jsc.emptyRDD();
        return JavaPairRDD.fromJavaRDD(empty);
    }

    public static <T> JavaRDD<T> unionAll(List<JavaRDD> rddList, SparkContext sc) {
        JavaRDD<T> acc = RDDUtils.getEmptyRDD(sc);
         for (JavaRDD<T> rdd : rddList)
             acc = acc.union(rdd);
         return acc;
    }

    public static <K, V> JavaPairRDD<K, V> unionAllPair(List<JavaPairRDD<K, V>> rddList, SparkContext sc) {
        JavaPairRDD<K, V> acc = RDDUtils.getEmptyPairRDD(sc);
        for (JavaPairRDD<K, V> rdd : rddList)
            acc = acc.union(rdd);
        return acc;
    }

    public static <K1, K2, V> JavaPairRDD<K1, Map<K2, V>> combineMapByKey(JavaPairRDD<K1, Map<K2, V>> rdd) {
        return rdd.reduceByKey((m1, m2) -> {
            m1.putAll(m2);
            return m1;
        });
    }

    public static <K1, K2, V> JavaPairRDD<K1, HashMap<K2, V>> combineHashMapByKey(JavaPairRDD<K1, HashMap<K2, V>> rdd) {
        return rdd.reduceByKey((m1, m2) -> {
            m1.putAll(m2);
            return m1;
        });
    }

    public static <K, T> JavaPairRDD<K, Collection<T>> combineCollectionByKey(JavaPairRDD<K, Collection<T>> rdd) {
        return rdd.reduceByKey((c1, c2) -> {
            c1.addAll(c2);
            return c1;
        });
    }

    public static class PropertyMapConverter implements
            PairFunction<
                        Tuple2<String, PropertyMap>,
                        String,
                        Map<String, JsonAST.JValue>
                    > {
        public Tuple2<String, Map<String, JsonAST.JValue>> call(Tuple2<String, PropertyMap> t) {
            Map<String, JsonAST.JValue> Jmap = JavaConverters.mapAsJavaMapConverter(t._2().fields()).asJava();
            return new Tuple2<>(t._1(), Jmap);
        }
    }
}
