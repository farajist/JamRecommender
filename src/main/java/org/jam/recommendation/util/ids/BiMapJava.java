package org.jam.recommendation.util.ids;

import org.apache.mahout.math.indexeddataset.BiMap;
import scala.Tuple2;
//import scala.collection.*;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

public class BiMapJava<K, V> {

    BiMap<K, V> bmap;

    public BiMapJava() {
    }

    public BiMapJava(BiMap bmap) {
        this.bmap = bmap;
    }

    public BiMapJava(HashMap<K, V> x) {
        Map xScala = toScalaImmutableMap(x);
        bmap = new BiMap<K,V>(xScala, null);
    }

    public BiMapJava<V,K> inverse() {
        return new BiMapJava<>(bmap.inverse());
    }

    public V get(K key) {
        return bmap.get(key).get();
    }

    public V getOrElse(K key, V dfault) {
        return bmap.get(key).get();
    }

    public boolean contains(K k) {
        return bmap.contains(k);
    }

    public V apply(K k){
        return bmap.apply(k);
    }

    public java.util.Map<K, V> toMap() {
        java.util.Map<K, V> mJava = JavaConverters.mapAsJavaMapConverter(bmap.toMap()).asJava();
        return mJava;
    }

    public int size() {
        return bmap.size();
    }

    public BiMapJava<K, V> take(int n) {
        return new BiMapJava<K, V>(bmap.take(n));
    }

    @Override
    public String toString() {
        return bmap.toString();
    }


    private <K, V> Map<K, V> toScalaImmutableMap(java.util.Map<K, V> jmap) {
        List<Tuple2<K, V>> tuples = jmap.entrySet()
                                        .stream()
                                        .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
                                        .collect(Collectors.toList());
        Seq<Tuple2<K, V>> scalaSeq = JavaConverters.asScalaIteratorConverter(tuples.iterator()).asScala().toSeq();
        return (Map<K, V>) Map$.MODULE$.apply(scalaSeq);

    }
}
