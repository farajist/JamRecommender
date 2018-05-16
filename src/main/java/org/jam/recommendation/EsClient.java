package org.jam.recommendation;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.predictionio.data.storage.DataMap;
import org.apache.predictionio.data.storage.Storage;


//import org.apache.predictionio.data.storage.elasticsearch.StorageClient;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchResponse;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.spark.rdd.EsSpark;
import org.jam.recommendation.util.StorageClient;
import org.joda.time.DateTime;
import org.json4s.JsonAST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;



import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EsClient {

    private transient static Logger logger = LoggerFactory.getLogger(EsClient.class);
    private static TransportClient client = null;

    private static final EsClient INSTANCE  = new EsClient();

    private EsClient() {

    }

    public static EsClient getInstance() {
        if (client == null) {
            if (Storage.getConfig("ELASTICSEARCH").nonEmpty())
                client = new StorageClient(Storage.getConfig("ELASTICSEARCH").get()).client();
            else
                throw new IllegalStateException(
                        "No elasticsearch client config detected, check your pio-env.sh for " +
                                "proper config settings");
        }
        return INSTANCE;
    }

    public boolean deleteIndex(String indexName, boolean refresh) {
        if (client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
            final DeleteIndexResponse delete = client.admin()
                    .indices().delete(new DeleteIndexRequest(indexName))
                    .actionGet();
            if (!delete.isAcknowledged()) {
                logger.info("Index " + indexName + "was not deleted, but may have quietly failed");
            } else {
                //now refresh to get it committed
                //todo : should do this after the new index is created so no index downtime
                if (refresh)
                    refreshIndex(indexName);
            }
            return true;
        } else {
            logger.warn("Elasticsearch index: " + indexName + "wasn't detected because it didn't exist. This may be an error !");
            return false;
        }
    }
    public boolean createIndex(
            String indexName, String indexType, List<String> fieldNames,
            Map<String, String> typeMappings,
            boolean refresh) {

        if(!client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
            final StringBuilder mappings = new StringBuilder();
            final String mappingsHead = "" +
                    "{" +
                    "   \"properties\": {";
            mappings.append(mappingsHead);

            for (String fieldName : fieldNames) {
                mappings.append(fieldName);
                if (typeMappings.containsKey(fieldName))
                    mappings.append(mappingsField(typeMappings.get(fieldName)));
                else
                    mappings.append(mappingsField("string"));
            }

            final String mappingsTail = "" +
                    "   \"id\": {" +
                    "       \"type\": \"string\"," +
                    "       \"index\": \"string\"," +
                    "       \"norms\": {" +
                    "           \"enabled\": false" +
                    "       }" +
                    "     }" +
                    "   }" +
                    "}";
            mappings.append(mappingsTail); //any other string is not analyzed

            final CreateIndexRequest cir = new CreateIndexRequest(indexName).mapping(indexType, mappings.toString());
            final CreateIndexResponse create = client.admin().indices().create(cir).actionGet();
            if (!create.isAcknowledged()) {
                logger.info("Index " + indexName + " was not created, but may have quietly failed.");
            } else {
                //now refresh to get it committed
                //todo : look up
                if (refresh)
                    refreshIndex(indexName);
            }

            return true;
        } else {
            logger.warn("Elasticsearch index: " + indexName + " was not created because it already exists. This maybe an error.");
            return false;
        }
    }

    private String mappingsField(String type) {
        return "" +
                "   :{" +
                "       \"type\": \"" + type + "\"," +
                "       \"index\": \"not_analyzed\"," +
                "       \"norms\" : {" +
                "           \"enable\" : false" +
                "       }" +
                "     },";
    }

    public void refreshIndex(String indexName) {
        client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
    }

    public void hotSwap(String alias, String typeName, JavaRDD<Map<String, Object>> indexRDD,
                        List<String> fieldNames, Map<String, String> typeMappings) {
        //get index for alias, change a char, create new id and index it, swap alias and delete old
        final ImmutableOpenMap<String, List<AliasMetaData>> aliasMetadata = client.admin()
                                                                            .indices().prepareGetAliases(alias)
                                                                            .get().getAliases();
        final String newIndex = alias + "_" + String.valueOf(DateTime.now().getMillis());

        logger.debug("Create new index: " + newIndex + ", " + typeName + ", " + fieldNames + ", " + typeMappings);

        final boolean refresh = false;
        final boolean response = createIndex(newIndex, typeName, fieldNames, typeMappings, refresh);

        final String newIndexURI = "/" + newIndex + "/" + typeName;

        final Map<String, String> m = new HashMap<>();
        m.put("es.mapping.id", "id");
        EsSpark.saveToEs(JavaRDD.toRDD(indexRDD), newIndexURI, scala.collection.JavaConverters.mapAsScalaMapConverter(m).asScala());

        if ((!aliasMetadata.isEmpty()) && (aliasMetadata.get(alias) != null) && (aliasMetadata.get(alias).get(0) != null)) {
            //was alias so remved the old one
            //append datetime to the alias to create an index name
            final String oldIndex = aliasMetadata.get(alias).get(0).getIndexRouting();
            client.admin().indices().prepareAliases()
                    .removeAlias(oldIndex, alias).addAlias(newIndex, alias).execute().actionGet();

            deleteIndex(oldIndex, refresh);
        } else {
            //todo: could be more than one index with 'alias' so no alias so add me
            // to clean up any indexes that exist with alias name
            final String indices = client.admin().indices().prepareGetIndex().get().indices()[0];
            if (indices.contains(alias))
                deleteIndex(alias, refresh);
            client.admin().indices()
                    .prepareAliases()
                    .addAlias(newIndex, alias)
                    .execute()
                    .actionGet();
        }

        //clean out any old index that were the product of a failed train
        final String indices = client.admin().indices().prepareGetIndex().get().indices()[0];
        if (indices.contains(alias) && !indices.equals(newIndex))
            deleteIndex(indices, false);

    }


    public SearchHits search(String query, String indexName) {
        QueryBuilder queryBuilder = new QueryStringQueryBuilder(query);
        final SearchResponse sr = client.prepareSearch(indexName).setSource(new SearchSourceBuilder().size(0).query(queryBuilder)).get();
        return sr.isTimedOut() ? null : sr.getHits();
    }

    public Map<String, Object> getSource(String indexName, String typeName, String doc) {
        return client.prepareGet(indexName, typeName, doc).execute().actionGet().getSource();
    }

    public String getIndexName(String alias) {
        final ImmutableOpenMap<String, List<AliasMetaData>> allIndicesMap = client.admin()
                                                                                .indices().getAliases(new GetAliasesRequest(alias))
                                                                                .actionGet()
                                                                                .getAliases();
        if (allIndicesMap.size() == 1) {
            String indexName = "";
            final Iterator<String> itr = allIndicesMap.keysIt();

            while(itr.hasNext())
                indexName = itr.next();
            //the one index that alias points to
            return indexName.equals("") ? null : indexName;
        } else {
            //delete all the indices that are pointed to by the alias, they can't be used
            logger.warn("There's no 1-1 mapping of index to alias so deleting the old indexes that are reference by\" "
            + "alias. This may have been caused by a crashed or stopped \"pio train \" op so try running it again");

            if (!allIndicesMap.isEmpty()) {
                final boolean refresh = true;
                for (ObjectCursor<String> indexName : allIndicesMap.keys())
                    deleteIndex(indexName.value, refresh);
            }

            return null; // if more than one abort, need to clean up bad aliases
        }
    }

    public JavaPairRDD<String, HashMap<String, JsonAST.JValue>> getRDD(String alias, String typeName, SparkContext sc) {
        final String indexName = getIndexName(alias);
        if (indexName == null || indexName.equals("")) {
            return null;
        } else {
            final String resource = alias + "/" + typeName;
            final JavaRDD<Tuple2<String, String>> tmp = EsSpark.esJsonRDD(sc, resource).toJavaRDD();
            final JavaPairRDD<String, String> esrdd = JavaPairRDD.fromJavaRDD(tmp);

            return esrdd.mapToPair(t ->
                    new Tuple2<String, HashMap<String, JsonAST.JValue>>(t._1(), new HashMap<>(
                            JavaConverters.mapAsJavaMapConverter(
                                    DataMap.apply(t._2()).fields()).asJava()))
            );
        }
    }
}
