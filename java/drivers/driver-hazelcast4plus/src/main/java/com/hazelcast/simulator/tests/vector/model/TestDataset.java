package com.hazelcast.simulator.tests.vector.model;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class TestDataset {

    private final float[][] searchVectors;

    private final int[][] closestIds;

    private final float[][] closestScores;

    public TestDataset(float[][] searchVector, int[][] closestIds, float[][] closestScores) {
        this.searchVectors = searchVector;
        this.closestIds = closestIds;
        this.closestScores = closestScores;
    }

    public float[] getSearchVector(int index) {
        return searchVectors[index];
    }

    public int getDimension() {
        if(searchVectors.length == 0) {
            return 0;
        }
        return searchVectors[0].length;
    }

    public int size() {
        return searchVectors.length;
    }

    public float getPrecisionV1(List<Integer> actualVectorsIds, int index, int top) {
        var actualSet = new HashSet<>(actualVectorsIds);
        var expectedSet = Arrays.stream(Arrays.copyOfRange(closestIds[index], 0, top)).boxed().collect(Collectors.toSet());
        actualSet.retainAll(expectedSet);
        return ((float) actualSet.size()) / top;
    }

    public float getPrecisionV2(float[] actualVectorsScore, int index) {
        var expected = Arrays.copyOfRange(closestScores[index], 0, actualVectorsScore.length);
        return distance(actualVectorsScore, expected);
    }

    private float distance(float[] array1, float[] array2) {
        double sum = 0f;
        for (int i = 0; i < array1.length; i++) {
            sum += Math.pow((array1[i] - array2[i]), 2.0);
        }
        return (float) Math.sqrt(sum);
    }
}