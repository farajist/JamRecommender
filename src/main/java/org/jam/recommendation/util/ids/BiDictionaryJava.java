package org.jam.recommendation.util.ids;

import org.apache.mahout.math.indexeddataset.BiDictionary;
import org.codehaus.janino.Java;
import scala.Predef$;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.JavaConverters$;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
//import java.util.Map;

public class BiDictionaryJava {

    BiDictionary bdict;

    public BiDictionaryJava(BiDictionary bdict) {
        this.bdict = bdict;
    }

    public BiDictionaryJava(List<String> l) {
        HashMap<String, Object> m = new HashMap<>();
        //this is stupid
        for (int i = 0; i < l.size(); ++i) {
            m.put(l.get(i), i);
        }

        Map<String, Object> xScala = convert(m);
        this.bdict = new BiDictionary(xScala, null);
    }

    public BiDictionaryJava merge(List<String> keys) {
        BiDictionary newBdict = bdict.merge(JavaConverters.collectionAsScalaIterableConverter(keys).asScala().toSeq());
        return new BiDictionaryJava(newBdict);
    }

    public BiMapJava inverse() {
        return new BiMapJava(bdict.inverse());
    }

    public Object get(String key) {
        return bdict.get(key).get();
    }

    public boolean contains(String s) {
        return bdict.contains(s);
    }

    public Object apply(String key) {
        return bdict.apply(key);
    }

    public java.util.Map<String, Object> toMap() {
        java.util.Map<String, Object> mJava = JavaConverters.mapAsJavaMapConverter(bdict.toMap()).asJava();
        return mJava;
    }

    public int size() {
        return bdict.size();
    }

    public BiMapJava take(int n) {
        return new BiMapJava(bdict.take(n));
    }

    public <K, V> Map<K, V> convert(java.util.Map<K, V> m) {
        List<Tuple2<K, V>> tuples = m.entrySet()
                .stream()
                .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        Seq<Tuple2<K, V>> scalaSeq = JavaConverters.asScalaIteratorConverter(tuples.iterator()).asScala().toSeq();
        return (Map<K, V>) Map$.MODULE$.apply(scalaSeq);
    }
}
