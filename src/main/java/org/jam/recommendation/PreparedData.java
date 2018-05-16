package org.jam.recommendation;

import org.apache.spark.api.java.JavaPairRDD;
import org.jam.recommendation.util.ids.IndexedDatasetJava;
import org.json4s.JsonAST;
import scala.Tuple2;


import java.io.Serializable;
import java.util.HashMap;
import java.util.List;


/** prepared data to be consumed by the JamAlgorithm
 * the difference between this and the training data is 
 * the conversion of rdd to indexeddataset .. for what reason ?
 * 
 */
public class PreparedData implements Serializable {
    
    private final List<Tuple2<String, IndexedDatasetJava>> actions;
    private final JavaPairRDD<String, HashMap<String, JsonAST.JValue>> fieldsRDD;

    public PreparedData(List<Tuple2<String, IndexedDatasetJava>> actions, 
                    JavaPairRDD<String, HashMap<String, JsonAST.JValue>> fieldsRDD) {
        this.actions = actions;
        this.fieldsRDD = fieldsRDD;
    }

    
    public JavaPairRDD<String, HashMap<String, JsonAST.JValue>> getFieldsRDD() {
        return this.fieldsRDD;
    }

    public List<Tuple2<String, IndexedDatasetJava>> getActions(){
        return this.actions;
    }
}