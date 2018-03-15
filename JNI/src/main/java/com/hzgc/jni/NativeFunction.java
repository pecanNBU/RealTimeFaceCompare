package com.hzgc.jni;

import com.hzgc.dubbo.feature.FaceAttribute;

import java.io.Serializable;

public class NativeFunction implements Serializable {
    /**
     * @param faceAttribute 人脸属性
     * @param data
     * @param width         图片宽度
     * @param height        图片高度
     * @return int 0:success 1：failure
     */
    public static native int feature_extract(FaceAttribute faceAttribute, int[] data, int width, int height);

    public static native void init();

    public static native void destory();

    public static native float compare(float[] currentFeature, float[] historyFeature);

    public static native void cublas_create();

    public static native void cu_malloc1();

    public static native float[] compare_gpu(int m, int n, int k, float[] javaData1, float[] javaData2);

    public static native void cu_free1();

    public static native void cublas_destroy();

    static {
        System.loadLibrary("FaceLib");
    }
}
