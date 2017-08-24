package com.hzgc.dubbo.dynamicrepo;

import java.util.List;
import java.util.Map;

public interface DynamicPhotoService {

    /**
     * 将rowKey、特征值插入人脸/车辆库 （内）（刘思阳）
     * 表名：person/car
     *
     * @param type    图片类型（人/车）
     * @param rowKey  图片id（rowkey）
     * @param feature 特征值
     * @return boolean 是否插入成功
     */
    boolean insertPictureFeature(PictureType type, String rowKey, float[] feature);

    /**
     * 根据小图rowKey获取小图特征值 （内）（刘思阳）
     * 表名：person/car
     *
     * @param imageId 小图rowKey
     * @param type    人/车
     * @return byte[] 小图特征值
     */
    byte[] getFeature(String imageId, PictureType type);

    List<float[]> getFeature(List<String> imageIdList, PictureType type);

    /**
     * 将上传的图片、rowKey、特征值插入人脸/车辆特征库 （内）
     * 表名：upFea
     *
     * @param type    人/车
     * @param rowKey  上传图片ID（rowKey）
     * @param feature 特征值
     * @param image   图片
     * @return boolean 是否插入成功
     */
    boolean upPictureInsert(PictureType type, String rowKey, float[] feature, byte[] image);

    /**
     * 将查询ID、查询相关信息插入查询结果库 （内）（刘思阳）
     * 表名：searchRes
     *
     * @param searchId 查询ID（rowKey）
     * @param resList  查询信息（返回图片ID、相识度）
     * @return boolean 是否插入成功
     */
    boolean insertSearchRes(String searchId, Map<String, Float> resList);

    /**
     * 根据动态库查询ID获取查询结果 （内）（刘思阳）
     * 表名：searchRes
     *
     * @param searchID 查询ID（rowKey）
     * @return search结果数据列表
     */
    Map<String, Float> getSearchRes(String searchID);

    /**
     * @param imageId
     * @param type
     * @return
     */
    CapturedPicture getCaptureMessage(String imageId, int type);

    /**
     * @param imageIdList
     * @param type
     * @return
     */
    List<CapturedPicture> getCaptureMessage(List<String> imageIdList, int type);
}
