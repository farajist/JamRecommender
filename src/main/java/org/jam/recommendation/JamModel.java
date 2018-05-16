package org.jam.recommendation;


import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.jam.recommendation.util.ids.IndexedDatasetJava;
import org.joda.time.DateTime;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/** JamRecommender models to save in ElasticSearch
 *
 */
public class JamModel {

    private transient static final Logger logger = LoggerFactory.getLogger(JamModel.class);

    private final List<Tuple2<String, IndexedDataset>> coocurrenceMatrices;
    private final List<JavaPairRDD<String, HashMap<String, JsonAST.JValue>>> propertiesRDDs;
    private final Map<String, String> typeMappings;
    private final boolean nullModel;
    private final SparkContext sc;

    public JamModel(List<Tuple2<String, IndexedDataset>> coocurrenceMatrices, List<JavaPairRDD<String,
                    HashMap<String, JsonAST.JValue>>> propertiesRDDs, Map<String, String> typeMappings,
                    boolean nullModel, SparkContext sc) {
        this.coocurrenceMatrices = coocurrenceMatrices;
        this.propertiesRDDs = propertiesRDDs;
        this.typeMappings = typeMappings;
        this.nullModel = nullModel;
        this.sc = sc;
    }

    /** save all fields to be indexed by ElasticSearch and queried for recs (algorithm),
     * this will be something like a table with row IDs = item IDs and separate fields
     * for all co-occurrence and cross-occurrence correlators and metadata for each time.
     * Metadata fields are limited to text term collections so vector types. Scalar
     * values can be used but depend on Elasticsearch's support. One exception is the data
     * scalar, which is also supported
     * @return always return true since most other reasons to not save cause no exceptions
     */

    /** The algorithm can predict only when data is indexed in a search engine like
     * Elasticsearch, save method will help store it
     */


    public boolean save(final List<String> dateNames, String esIndex, String esType) {
        logger.debug("Start save model");

        if (nullModel)
            throw new IllegalStateException("Saving a null model created from loading an old one.");

        /**for ElasticSearch we need to create the entire index in an rdd of maps, one per
         * item so we'll use convert co-occurrence matrices into correlators as
         * RDD[(itemID, (actionName, Seq[itemID])] so they need to be in Elasticsearch format '_-'
         */


        //must know what the algorithm produces and how it creates concurrence matrices
        //must be in the format : List of Tuple2(string, indexeddataset)
        final List<JavaPairRDD<String, HashMap<String, JsonAST.JValue>>> correlatorRDDs = new LinkedList<>();

        for (Tuple2<String, IndexedDataset> t : this.coocurrenceMatrices) {
            //remember ! seen those before right ! @prepared data right ?
            final String actionName = t._1();
            final IndexedDataset dataset = t._2();

            //some sanity check here ?

            final IndexedDatasetSpark idSpark = (IndexedDatasetSpark) dataset;
            final IndexedDatasetJava idJava = new IndexedDatasetJava(idSpark);

            //sanity check #2

            final Conversions.IndexedDatasetConversions idConvert = new Conversions.IndexedDatasetConversions(idJava);
            correlatorRDDs.add(idConvert.toStringMapRDD(actionName));
        }

        logger.info("Group all properties RDD");

        final List<JavaPairRDD<String, HashMap<String, JsonAST.JValue>>> allRDDs = new LinkedList<>();
        //How did we build both correlatorRDDs and the propertiesRDD and what's inside
        allRDDs.addAll(correlatorRDDs);
        allRDDs.addAll(propertiesRDDs);

        final JavaPairRDD<String, HashMap<String, JsonAST.JValue>> groupedRDD = groupAll(allRDDs);
        final JavaRDD<Map<String, Object>> esRDD = groupedRDD.mapPartitions(iter -> {
                    final List<Map<String, Object>> result = new LinkedList<>();
                    while (iter.hasNext()) {
                        final Tuple2<String, HashMap<String, JsonAST.JValue>> t = iter.next();
                        final String itemId = t._1();
                        final HashMap<String, JsonAST.JValue> itemProps = t._2();
                        final Map<String, Object> propsMap = new HashMap<>();

                        for (Map.Entry<String, JsonAST.JValue> entry : itemProps.entrySet()) {
                            final String propName = entry.getKey();
                            final JsonAST.JValue propValue = entry.getValue();
                            propsMap.put(propName, JamModel.extractJValue(dateNames, propName, propValue));
                        }
                        propsMap.put("id", itemId);
                        result.add(propsMap);
                    }
                    return result.iterator();
                }
        );
        if (esRDD == null) {
            throw new IllegalStateException("esRDD is null and this is a hassle :/");

        }
        //is it gonna work
        final List<String> esFields = esRDD.flatMap(s -> s.keySet().iterator()).distinct().collect();
        logger.info("ES fields [" + esFields.size() + "]:" + esFields);

        EsClient.getInstance().hotSwap(esIndex, esType, esRDD, esFields, typeMappings);
        return true;
    }

    private JavaPairRDD<String,HashMap<String,JsonAST.JValue>> groupAll(
            List<JavaPairRDD<String,HashMap<String,JsonAST.JValue>>> fields) {
        final JavaPairRDD<String, HashMap<String, JsonAST.JValue>> tmp = RDDUtils.unionAllPair(fields, sc);
        return RDDUtils.combineHashMapByKey(tmp);
    }

    private static Object extractJValue(List<String> dateNames, String key, JsonAST.JValue value) {
        if (value instanceof JsonAST.JArray) {
            final List<Object> list = new LinkedList<>();
            JsonAST.JArray temp = (JsonAST.JArray) value;
            int counter = 0;
            while (counter < temp.values().size()) {
                list.add(extractJValue(dateNames, key, temp.apply(counter)));
                counter++;
            }
            return list;
        } else if (value instanceof JsonAST.JString) {
            final String s = ((JsonAST.JString) value).s();

            if (dateNames.contains(key))
                return new DateTime(s).toDate();
            else if (RankingFieldName.toList().contains(key))
                return Double.parseDouble(s);
            else
                return s;
        } else if (value instanceof JsonAST.JDouble) {
            return ((JsonAST.JDouble) value).num();
        } else if (value instanceof JsonAST.JInt) {
            return ((JsonAST.JInt) value).num();
        } else if (value instanceof JsonAST.JBool) {
            return ((JsonAST.JBool) value).value();
        } else {
            return value;
        }
    }

}
