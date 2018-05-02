package org.jam.recommendation.evaluation;

import org.apache.predictionio.controller.Engine;
import org.apache.predictionio.controller.java.JavaEvaluation;
import org.apache.predictionio.core.BaseAlgorithm;
import org.jam.recommendation.Algorithm;
import org.jam.recommendation.DataSource;
import org.jam.recommendation.PredictedResult;
import org.jam.recommendation.Preparator;
import org.jam.recommendation.PreparedData;
import org.jam.recommendation.Query;
import org.jam.recommendation.Serving;

import java.util.Collections;

public class EvaluationSpec extends JavaEvaluation {
    public EvaluationSpec() {
        this.setEngineMetric(
                new Engine<>(
                        DataSource.class,
                        Preparator.class,
                        Collections.<String, Class<? extends BaseAlgorithm<PreparedData, ?, Query, PredictedResult>>>singletonMap("jam", Algorithm.class),
                        Serving.class
                ),
                new PrecisionMetric()
        );
    }
}
