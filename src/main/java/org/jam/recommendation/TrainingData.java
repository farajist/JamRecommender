package org.jam.recommendation;

import org.apache.predictionio.controller.SanityCheck;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.predictionio.data.storage.PropertyMap;
import scala.Tuple2;


import java.io.Serializable;
import java.util.List;

/** Low level RDD based representation of the data ready for the preparator
 * @author farajist
 * 
 * @param actions List of Tuples (actionName, actionRDD)
 * @param fieldsRDD RDD of item keyed PropertyMap for item metadata
 */
public class TrainingData implements Serializable, SanityCheck {
    
    private final List<Tuple2<String, JavaPairRDD<String, String>>> actions;
    private final JavaPairRDD<String, PropertyMap> fieldsRDD;

    public TrainingData(List<Tuple2<String, JavaPairRDD<String, String>>> actions, JavaPairRDD<String, PropertyMap> fieldsRDD) {
        this.actions = actions;
        this.fieldsRDD = fieldsRDD;
    }

    public List<Tuple2<String, JavaPairRDD<String, String>>> getActions() {
        return this.actions;
    }

    public JavaPairRDD<String, PropertyMap> getFieldsRDD() {
        return this.fieldsRDD;
    }

    @Override
    public void sanityCheck() {
        if (actions.isEmpty()) {
            throw new AssertionError("Actions data is empty");
        }
        if (fieldsRDD.isEmpty()) {
            throw new AssertionError("Fields RDD data is empty");
        }
    }
}
