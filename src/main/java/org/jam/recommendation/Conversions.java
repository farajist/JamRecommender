package org.jam.recommendation;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.drm.CheckpointedDrm;
import org.apache.mahout.math.indexeddataset.BiDictionary;
import org.apache.mahout.sparkbindings.SparkDistributedContext;
import org.apache.mahout.sparkbindings.drm.CheckpointedDrmSparkOps;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.jam.recommendation.util.ids.BiDictionaryJava;
import org.jam.recommendation.util.ids.IndexedDatasetJava;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.*;

public class Conversions {

    public static void drawJamRecommender(Logger logger) {
        final String jamRec = "";
        logger.info(jamRec);
    }

    public static void drawInfo(String title, List<Tuple2<String, Object>> dataMap,
                                Logger logger) {
        final String leftAlignFormat = "| %-30s%-28s |";

        final String line = strMul("=", 60);

        final String preparedTitle = String.format ("| %-58 |", title);

        final StringBuilder data = new StringBuilder();
        for (Tuple2<String, Object> t : dataMap) {
            data.append(String.format(leftAlignFormat, t._1, t._2));
            data.append("\n\t");
        }

        final String info = "" +
                "\n\t╔" + line + "╗" +
                "\n\t" + preparedTitle +
                "\n\t" + data.toString().trim() +
                "\n\t╚" + line + "╝" +
                "\n\t";
        logger.info(info);
    }

    public static String strMul(String str, int n) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; ++i)
            sb.append(str);
        return sb.toString();
    }

    public static class OptionCollection<T> {
        private final Optional<List<T>> collectionOpt;

        public OptionCollection(Optional<List<T>> collectionOpt) {
            this.collectionOpt = collectionOpt;
        }

        public List<T> getOrEmpty() {
            if (!collectionOpt.isPresent()) {
                return new ArrayList<>();
            }
            return collectionOpt.get();
        }
    }

    public static class IndexedDatasetConversions {

        private final IndexedDatasetJava indexedDataset;

        private transient final Logger logger = LoggerFactory.getLogger(IndexedDatasetConversions.class);

        public IndexedDatasetConversions(IndexedDatasetJava indexedDataset) {
            this.indexedDataset = indexedDataset;
        }

        public JavaPairRDD<String, HashMap<String, JsonAST.JValue>> toStringMapRDD
                (final String actionName) {
            final BiDictionaryJava rowIDDictionary = indexedDataset.getRowIds();
            final SparkContext sc = ((SparkDistributedContext) indexedDataset.getMatrix().context()).sc();
            final ClassTag<BiDictionaryJava> tag = ClassTag$.MODULE$.apply(BiDictionaryJava.class);
            final Broadcast<BiDictionaryJava> rowIDDictionary_bcast = sc.broadcast(rowIDDictionary, tag);

            final BiDictionaryJava columnIDDictionary = indexedDataset.getColIds();
            final Broadcast<BiDictionaryJava> columnIDDictionary_bcast = sc.broadcast(columnIDDictionary, tag);

            final CheckpointedDrm<Object> temp = indexedDataset.matrix();
            CheckpointedDrmSparkOps<Object> temp2 = new CheckpointedDrmSparkOps<>(temp);
            JavaPairRDD<Object, Vector> temp3 = JavaPairRDD.fromJavaRDD(temp2.rdd().toJavaRDD());

            return temp3.mapToPair(entry -> {
                final int rowNum = (Integer) entry._1();
                final Vector itemVector = entry._2();

                //turns non-zeros into list for sorting
                List<Tuple2<Integer, Double>> itemList = new ArrayList<>();
                for (Vector.Element ve : itemVector.nonZeroes()) {
                    itemList.add(new Tuple2<>(ve.index(), ve.get()));
                }

                //sort by highest strength value descending(-)
                Comparator<Tuple2<Integer, Double>> c =
                        (elt1, elt2) -> (new Double(elt1._1().doubleValue() * -1.0))
                                    .compareTo(new Double(elt2._2().doubleValue() * -1.0));
                itemList.sort(c);
                List<Tuple2<Integer, Double>> vector = itemList;

                final String invalid = "INVALID_ITEM_ID";
                final Object itemID = rowIDDictionary_bcast.value().inverse().getOrElse(rowNum, invalid);
                final String itemId = itemID.toString();

                try {
                    //equivalent to Predef.require
                    if(itemId.equals(invalid)) {
                        throw new IllegalArgumentException("Bad row number in matrix, skipping item " + rowNum);
                    }

                    //equivalent to Predef.require #2
                    if (vector.isEmpty()) {
                        throw new IllegalArgumentException("No values skipping item " + rowNum);
                    }

                    //create a list of item ids
                    List<JsonAST.JValue> holderList = new ArrayList<>();
                    for (Tuple2<Integer, Double> element : vector) {
                        holderList.add(new JsonAST.JString(columnIDDictionary_bcast
                                        .value()
                                        .inverse()
                                        .getOrElse(element._1(), "").toString()));
                    }

                    scala.collection.immutable.List<JsonAST.JValue> tmp = JavaConverters
                            .asScalaBufferConverter(holderList)
                            .asScala()
                            .toList();
                    JsonAST.JArray values = new JsonAST.JArray(tmp);
                    HashMap<String, JsonAST.JValue> rtnMap = new HashMap<>();
                    rtnMap.put(actionName, values);
                    return new Tuple2<>(itemId, rtnMap);
                } catch (IllegalArgumentException iae) {
                    return null;
                }
            }).filter(Objects::nonNull);

        }
    }
}
