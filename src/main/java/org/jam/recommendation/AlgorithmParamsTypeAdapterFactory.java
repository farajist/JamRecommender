package org.jam.recommendation;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AlgorithmParamsTypeAdapterFactory<C> implements TypeAdapterFactory {


    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        return type.getRawType() == JamAlgorithmParams.class
                ? (TypeAdapter<T>) customizeMyClassAdapter(gson, (TypeToken<C>) type)
                : null;
    }

    private TypeAdapter<C> customizeMyClassAdapter(Gson gson, TypeToken<C> type) {
        final TypeAdapter<C> delegate = gson.getDelegateAdapter(this, type);
        final TypeAdapter<JsonElement> elementAdapter = gson.getAdapter(JsonElement.class);
        return new TypeAdapter<C>() {
            @Override
            public void write(JsonWriter out, C value) throws IOException {
                JsonElement tree = delegate.toJsonTree(value);
                elementAdapter.write(out, tree);
            }

            @Override
            public C read(JsonReader in) throws IOException {
                JsonElement tree = elementAdapter.read(in);

                //extract JamAlgorithmParams
                String appName = ((JsonObject) tree).get("appName") != null ? ((JsonObject) tree).get("appName").getAsString() : null;
                String indexName = ((JsonObject) tree).get("indexName") != null ? ((JsonObject) tree).get("indexName").getAsString() : null;
                String typeName = ((JsonObject) tree).get("typeName") != null ? ((JsonObject) tree).get("typeName").getAsString() : null;
                String recsModel = ((JsonObject) tree).get("recsModel") != null ? ((JsonObject) tree).get("recsModel").getAsString() : null;

                List<String> eventNames = ((JsonObject) tree).get("eventNames") != null ? Arrays.asList(new Gson().fromJson(((JsonObject) tree).get("eventNames").getAsJsonArray(), String[].class)) : new ArrayList<>();
                List<String> blacklistEventNames = ((JsonObject) tree).get("blacklistEventNames") != null ? Arrays.asList(new Gson().fromJson(((JsonObject) tree).get("blacklistEventNames").getAsJsonArray(), String[].class)) : new ArrayList<>();

                Integer maxQueryEvents = ((JsonObject) tree).get("maxQueryEvents") != null ? ((JsonObject) tree).get("maxQueryEvents").getAsInt() : null;
                Integer maxEventsPerEventType = ((JsonObject) tree).get("maxEventsPerEventType") != null ? ((JsonObject) tree).get("maxEventsPerEventType").getAsInt() : null;
                Integer maxCorrelatorsPerEventType = ((JsonObject) tree).get("maxCorrelatorsPerEventType") != null ? ((JsonObject) tree).get("maxCorrelatorsPerEventType").getAsInt() : null;
                Integer num = ((JsonObject) tree).get("num") != null ? ((JsonObject) tree).get("num").getAsInt() : null;

                Float userBias = ((JsonObject) tree).get("userBias") != null ? ((JsonObject) tree).get("userBias").getAsFloat() : null;
                Float itemBias = ((JsonObject) tree).get("itemBias") != null ? ((JsonObject) tree).get("itemBias").getAsFloat() : null;

                Boolean returnSelf = ((JsonObject) tree).get("returnSelf") != null ? ((JsonObject) tree).get("returnSelf").getAsBoolean() : null;

                List<Field> fields = ((JsonObject) tree).get("fields") != null ? Arrays.asList(new Gson().fromJson(((JsonObject) tree).get("fields").getAsJsonArray(), Field[].class)) : new ArrayList<>();
                List<RankingParams> rankingParams = ((JsonObject) tree).get("rankingParams") != null ? Arrays.asList(new Gson().fromJson(((JsonObject) tree).get("rankingParams").getAsJsonArray(), RankingParams[].class)) : new ArrayList<>();

                String availableDateName = ((JsonObject) tree).get("availableDateName") != null ? ((JsonObject) tree).get("availableDateName").getAsString() : null;
                String expireDateName = ((JsonObject) tree).get("expireDateName") != null ? ((JsonObject) tree).get("expireDateName").getAsString() : null;
                String dateName = ((JsonObject) tree).get("dateName") != null ? ((JsonObject) tree).get("dateName").getAsString() : null;

                List<IndicatorParams> indicatorParams = ((JsonObject) tree).get("indicatorParams") != null ? Arrays.asList(new Gson().fromJson(((JsonObject) tree).get("indicatorParams").getAsJsonArray(), IndicatorParams[].class)) : new ArrayList<>();

                Long seed = ((JsonObject) tree).get("seed") != null ? ((JsonObject) tree).get("seed").getAsLong() : null;

                // return using constructor
                return (C) new JamAlgorithmParams(appName, indexName, typeName, recsModel, eventNames, blacklistEventNames, maxQueryEvents,
                        maxEventsPerEventType, maxCorrelatorsPerEventType, num, userBias, itemBias, returnSelf, fields, rankingParams,
                        availableDateName, expireDateName, dateName, indicatorParams, seed);
            }
        };
    }
}
