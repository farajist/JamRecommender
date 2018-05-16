package org.jam.recommendation;

import java.util.*;

import org.apache.predictionio.controller.java.PJavaPreparator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.jam.recommendation.util.ids.BiDictionaryJava;
import org.jam.recommendation.util.ids.IndexedDatasetJava;
import org.json4s.JsonAST;
import scala.Tuple2;
import scala.collection.JavaConverters;

public class Preparator extends PJavaPreparator<TrainingData, PreparedData> {

   @Override
   public PreparedData prepare(SparkContext sc, TrainingData trainingData) {
        /**
         * now we have all actions (user events) in separate RDDs we
         * must merge any user dictionaries (mappings) and make sure the same 
         * ids map correct events
         * */

        Optional<BiDictionaryJava> userDictionary = Optional.empty();

        List<Tuple2<String, IndexedDatasetJava>> indexedDatasets = new ArrayList<>();

        //make sure the same user ids map to correct events for merged dicts
        for (Tuple2<String, JavaPairRDD<String, String>> entry : trainingData.getActions()) {
            String eventName = entry._1();
            JavaPairRDD<String, String> eventIDs = entry._2();

            /**
             * passing in previous row dictionary will use values() 
             * if they exist, and append any new ids, so after all
             * are constructed we have all user ids in the last dictionary
             */
            IndexedDatasetJava ids = IndexedDatasetJava.apply(eventIDs, userDictionary, sc);
            //after this maybe we have each user now mapping to his events
            userDictionary = Optional.of(ids.getRowIds());

            //append transformation to indexedDataSets list
            indexedDatasets.add(new Tuple2(eventName, ids));
        }

        List<Tuple2<String, IndexedDatasetJava>> rowAdjustedIds = new ArrayList<>();

        //check to see if there are events in primary event IndexedDataset
        //return empty list if not

        if (userDictionary.isPresent()) {
            /**
             * now make sure all matrices have identical row space since this 
             * corresponds to all users with primary event since other users do
             * no contribute to the math 
             * **/
            for (Tuple2<String, IndexedDatasetJava> entry : indexedDatasets) {
                String eventName = entry._1();
                IndexedDatasetJava eventIDs = entry._2();
                rowAdjustedIds.add(new Tuple2<>(eventName,
                        (new IndexedDatasetJava(eventIDs.getMatrix(), userDictionary.get(),
                                        eventIDs.getColIds()))
                            .newRowCardinality(userDictionary.get().size())));
            }
        }

        JavaPairRDD<String, Map<String, JsonAST.JValue>> fieldsRDD =
            trainingData.getFieldsRDD().mapToPair(entry -> new Tuple2<>(
                entry._1(), JavaConverters.mapAsJavaMapConverter(entry._2().fields())
                .asJava()));
        JavaPairRDD<String, HashMap<String, JsonAST.JValue>> fields = fieldsRDD.mapToPair(entry ->
            new Tuple2<>(entry._1(), new HashMap<>(entry._2())));

        return new PreparedData(rowAdjustedIds, fields);
   }


}


 /**
  * This is a companion object used to build an [[org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark]]
  * The most important odditiy is that it takes a BiDictionary of row-ids optionally. If provided no row with another
  * id will be added to the dataset. This is useful for cooccurrence type calculations where all arrays must have
  * the same rows and there is some record of which rows are important.
  * */

