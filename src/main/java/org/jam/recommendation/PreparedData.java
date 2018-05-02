package org.jam.recommendation;

import java.io.Serializable;

public class PreparedData implements Serializable {
    private final TrainingData trainingData;

    public PreparedData(TrainingData trainingData) {
        this.trainingData = trainingData;
    }

    public TrainingData getTrainingData() {
        return trainingData;
    }
}
