package com.hzgc.jni;

public class ClusteringFunction {
    /**
     * permanent resident population calculate
     *
     * @param features features array
     * @param dataSize features count
     * @param fileName file name year-month.txt(2018-02.txt)
     * @param filePath absolute path of the results file
     * @return 0 or 1, 1 success, 0 fail
     */
    public static native int clusteringComputer(float[] features, int dataSize, String fileName, String filePath);

    static {
        System.loadLibrary("resident");
    }

    public static void main(String[] args) {
        ClusteringFunction.clusteringComputer(new float[0], 0, "pp", "pp");
        System.out.println(System.getProperty("java.library.path"));
    }
}
