package org.jam.recommendation.util.ids;

import org.apache.mahout.math.drm.CheckpointedDrm;
import org.apache.mahout.math.drm.DistributedContext;
import org.apache.mahout.math.indexeddataset.BiDictionary;
import org.apache.mahout.math.indexeddataset.IndexedDataset;
import org.apache.mahout.math.indexeddataset.Schema;
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark;
import org.apache.predictionio.data.store.java.OptionHelper;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Optional;

public class IndexedDatasetJava implements IndexedDataset {

    private IndexedDatasetSpark ids;

    public IndexedDatasetJava(CheckpointedDrm drm, BiDictionaryJava rowIds, BiDictionaryJava colIds) {
        ids = new IndexedDatasetSpark(drm, rowIds.bdict, colIds.bdict);
    }


    public IndexedDatasetJava(IndexedDatasetSpark ids) {
        this.ids = ids;
    }

    @Override
    public CheckpointedDrm<Object> matrix() {
        return ids.matrix();
    }

    @Override
    public BiDictionary rowIDs() {
        return ids.rowIDs();
    }

    @Override
    public BiDictionary columnIDs() {
        return ids.columnIDs();
    }

    @Override
    public void dfsWrite(String dest, Schema schema, DistributedContext sc) {
        ids.dfsWrite(dest, schema, sc);
    }

    @Override
    public IndexedDataset create(CheckpointedDrm<Object> matrix, BiDictionary rowIDs, BiDictionary columnIDs) {
        return ids.create(matrix, columnIDs, rowIDs);
    }

    @Override
    public IndexedDatasetJava newRowCardinality(int n) {
        return new IndexedDatasetJava(ids.matrix().newRowCardinality(n),
                new BiDictionaryJava(ids.rowIDs()),
                new BiDictionaryJava(ids.columnIDs()));
    }

    public CheckpointedDrm getMatrix() {
        return ids.matrix();
    }

    public BiDictionaryJava getColIds() {
        return new BiDictionaryJava(ids.columnIDs());
    }

    public BiDictionaryJava getRowIds() { return new BiDictionaryJava(ids.rowIDs()); }
    public static IndexedDatasetJava apply(JavaPairRDD<String, String> rdd,
                                           Optional<BiDictionaryJava> existingRowIDs,
                                           SparkContext sc) {

        IndexedDatasetSpark newIds;

        newIds = existingRowIDs.map(biDictionaryJava -> IndexedDatasetSpark.apply(rdd.rdd(),
                OptionHelper.some(biDictionaryJava.bdict), sc)).orElseGet(() -> IndexedDatasetSpark.apply(rdd.rdd(),
                OptionHelper.none(), sc));
        return new IndexedDatasetJava(newIds);
    }


}
