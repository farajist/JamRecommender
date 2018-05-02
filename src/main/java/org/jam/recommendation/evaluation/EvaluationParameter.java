package org.jam.recommendation.evaluation;

import org.apache.predictionio.controller.EmptyParams;
import org.apache.predictionio.controller.EngineParams;
import org.apache.predictionio.controller.java.JavaEngineParamsGenerator;
import org.jam.recommendation.AlgorithmParams;
import org.jam.recommendation.DataSourceParams;

import java.util.Arrays;
import java.util.Collections;

public class EvaluationParameter extends JavaEngineParamsGenerator {
    public EvaluationParameter() {
        this.setEngineParamsList(
                Collections.singletonList(
                        new EngineParams(
                                "",
                                new DataSourceParams("javadase"),
                                "",
                                new EmptyParams(),
                                Collections.singletonMap("jam", new AlgorithmParams(1, 10, 10, 0.01, "javadase", Collections.singletonList("view"), true, Arrays.asList("buy", "view"))),
                                "",
                                new EmptyParams()
                        )
                )
        );
    }
}
