package com.hzgc.hbase.dynamicrepo;

import com.hzgc.dubbo.dynamicrepo.*;
import com.hzgc.jni.Attribute.*;
import com.hzgc.jni.FaceAttr;
import com.hzgc.jni.FaceFunction;
import com.hzgc.util.ObjectListSort.ListUtils;
import com.hzgc.util.UuidUtil;
import com.hzgc.util.jdbc.JDBCUtil;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by Melody on 2017-10-28.
 */
public class RealTimeCompareBySparkSQL {

    private Logger LOG = Logger.getLogger(RealTimeCompare.class);
    /**
     * 图片的二进制数据
     */
    private byte[] image;
    /**
     * 图片 id ,优先使用图片流数组
     */
    private String imageId;
    /**
     * 阈值
     */
    private float threshold;
    /**
     * 排序参数
     */
    private String sortParams;
    /**
     * 分页查询开始行
     */
    private int offset;
    /**
     * 分页查询条数
     */
    private int count;
    /**
     * 查询Id 由UUID生成
     */
    private String searchId;
    /**
     * 用于保存筛选出来的一组一个图片的id
     */
    private List<String> imageIdList;
    /**
     * 过滤大图后的图片Id列表
     */
    private List<String> imageIdFilterList;
    /**
     * 查询结果，最终的返回值
     */
    private SearchResult searchResult = new SearchResult();
    /**
     * 特征列表，根据rowKeyList批量查询到的特征
     */
    private List<float[]> feaFloatList;
    /**
     * 相似度列表，保存比对后的相似度
     */
    private List<Float> simList;
    /**
     * 图片对象列表
     */
    private List<CapturedPicture> capturedPictureList;
    private CapturedPicture capturedPicture;
    private DynamicPhotoService dynamicPhotoService;
    private String insertType;
    /**
     * 人脸属性对象
     */
    private FaceAttr attribute;
    /**
     * 临时保存通过图片id查询出来的特征值
     *
     * */
    private String featureByImageId;

    public RealTimeCompareBySparkSQL() {
        dynamicPhotoService = new DynamicPhotoServiceImpl();
    }


    SearchResult pictureSearchBySparkSQL(SearchOption option) {

        JDBCUtil jdbcUtil = JDBCUtil.getInstance();

        if (null != option) {
            //搜索类型 是人还是车
            SearchType searchType = option.getSearchType();
            //阈值
            threshold = option.getThreshold();
            //排序参数
            sortParams = option.getSortParams();
            //上传图片数据
            image = option.getImage();
            //图片id
            imageId=option.getImageId();
            //分页查询开始行
            offset = option.getOffset();
            //分页查询数
            count = option.getCount();
            //平台车牌号
            String plateNumber = option.getPlateNumber();
            //平台id
            String platformId = option.getPlatformId();
            //设备id列表
            List<String> ipcId = option.getDeviceIds();
            // 起始时间
            Date startTime = option.getStartDate();
            // 结束时间
            Date endTime = option.getEndDate();
            // 时间段
            List<TimeInterval> timeIntervals = option.getIntervals();
            /*//人脸属性
            attribute = option.getAttribute();
            //是否戴眼镜
            Eyeglasses eyeglasses = attribute.getEyeglasses();
            //性别
            Gender gender = attribute.getGender();
            //头发颜色
            HairColor hairColor = attribute.getHairColor();
            //头发类型
            HairStyle hairStyle = attribute.getHairStyle();
            //是否戴帽子
            Hat hat = attribute.getHat();
            //胡子类型
            Huzi huzi = attribute.getHuzi();
            //是否系领带
            Tie tie = attribute.getTie();*/

            //设置查询Id
            searchId = UuidUtil.setUuid();
            if (null != searchType) {
                //查询的对象库是人
                if (searchType == SearchType.PERSON) {
                    insertType = DynamicTable.PERSON_TYPE;
                    PictureType pictureType = PictureType.SMALL_PERSON;
                    if (null != image && image.length > 0) {
                        //提取上传图片的特征值
                        float[] searchFea = FaceFunction.featureExtract(image).getFeature();
                        //将图片特征插入到特征库
                        boolean insertStatus = dynamicPhotoService.upPictureInsert(pictureType, searchId, searchFea, image);

                        if (null != searchFea && searchFea.length == 512) {
                            //将float[]特征值转为String特征值
                            String searchFeaStr = FaceFunction.floatArray2string(searchFea);
                            //查询需要传入的参数
                            //阈值/排序参数/分页查询开始行/分页查询数startTime  endTime , offset, count
                            Object[] params = {threshold};
                            //添加排序条件
                            String sortParamsNew = ListUtils.getOrderStringBySort1(sortParams);
                            //根据条件过滤计算符合条件的总数
                            String selectCount = "select * from " +
                                    "(select * from " +
                                    "( select * ,com(" + searchFeaStr + " ,feature) as similarity from person_table ) " +
                                    "temp3 " +
                                    "where similarity >= ?" +
//                                    "and startTime = ?" +
//                                    "and endTime = ?" +
//                                    "and ipc in ({0})" +
                                    "order by " + sortParamsNew + " " +
                                    ") temp4";
                            String formatted = String.format(selectCount, String.join(",", ipcId));
                            jdbcUtil.executeQuery(selectCount, params, new JDBCUtil.QueryCallback() {
                                @Override
                                public void process(ResultSet rs) throws Exception {
                                    while (rs.next()) {
                                        int sum = rs.getInt(1);
                                        //rowkey
                                        String imageId = rs.getString(1);
                                        //设备id
                                        String ipcId = rs.getString(2);
                                        //相似度
                                        Float similarity = rs.getFloat(3);
                                        //时间戳
                                        Long timeStamp = rs.getLong(4);
                                        capturedPicture = new CapturedPicture();
                                        capturedPicture.setId(imageId);
                                        capturedPicture.setIpcId(ipcId);
                                        capturedPicture.setTimeStamp(timeStamp);
                                        capturedPicture.setSimilarity(similarity);
                                    }
                                }
                            });
                            capturedPictureList = new ArrayList<>();
                            capturedPictureList.add(capturedPicture);
                            boolean flag = dynamicPhotoService.insertSearchRes(searchId, capturedPictureList, insertType);
                            if (flag) {
                                LOG.info("The search history of: [" + searchId + "] saved successful");
                            } else {
                                LOG.error("The search history of: [" + searchId + "] saved failure");
                            }
                            searchResult.setSearchId(searchId);
                            searchResult.setPictures(capturedPictureList);
                            //根据条件过滤并根据条件分组排序分页显示
//                            String selectSQL = "select temp3.* from " +
//                                    "(select *,row_number() over(order by " + sortParamsNew + ") rn from " +
//                                    "(select * from " +
//                                    "(select *,com(" + searchFeaStr + ",feature) as similarity " +
//                                    "from person_table) temp1 " +
//                                    "where similarity >= ? " +
//                                    "and startTime = ?" +
//                                    "and endTime = ?" +
//                                    "and ipc in ({0})" +
//                                    ") " + "temp2 ) " + "temp3" +
//                                    "where temp3.rn >= ? and temp3.rn <= ? ";
//                            String formatted = String.format(selectSQL, String.join(",", ipcId));

//                            jdbcUtil.executeQuery(formatted, params, new JDBCUtil.QueryCallback() {
//                                @Override
//                                public void process(ResultSet rs) throws Exception {
//                                    while (rs.next()) {
//                                        //rowkey
//                                        String imageId = rs.getString(1);
//                                        //设备id
//                                        String ipcId = rs.getString(2);
//                                        //相似度
//                                        Float similarity = rs.getFloat(3);
//                                        //时间戳
//                                        Long timeStamp = rs.getLong(4);
//
//                                        capturedPicture = new CapturedPicture();
//                                        capturedPicture.setId(imageId);
//                                        capturedPicture.setIpcId(ipcId);
//                                        capturedPicture.setTimeStamp(timeStamp);
//                                        capturedPicture.setSimilarity(similarity);
//                                    }
//                                }
//                            });
//                            capturedPictureList = new ArrayList<>();
//                            capturedPictureList.add(capturedPicture);
//                            searchResult.setSearchId(searchId);
//                            searchResult.setPictures(capturedPictureList);
                        } else {
                            LOG.info("search feature is null or short than 512");
                        }
                    } else {
                        //无图，有imageId
                        if (null != imageId) {
                            //添加过滤条件
                            String sortParamsNew = ListUtils.getOrderStringBySort1(sortParams);
                            //查询需要传入的参数
                            //阈值/排序参数/分页查询开始行/分页查询数
                            Object[] params = {threshold};
                            String selectFeatureById = "select feature from person_table where  rowkey = "+imageId+" ";
                            jdbcUtil.executeQuery(selectFeatureById, params, new JDBCUtil.QueryCallback() {
                                @Override
                                public void process(ResultSet rs) throws Exception {
                                    //临时保存特征值
                                     featureByImageId = rs.getString(1);
                                }
                            });

                            String selecttest = "select * from " +
                                    "(select * from " +
                                    "( select * ,com(" + featureByImageId + " ,feature) as similarity from person_table ) " +
                                    "temp3 " +
                                    "where similarity >= ?" +
//                                    "and startTime = ?" +
//                                    "and endTime = ?" +
                                    "and ipc in ({0})" +
                                    "order by " + sortParamsNew + "" +
                                    ") temp4";

//                            String selectByFeature = "select temp3.* from " +
//                                    "(select *,row_number() over(order by " + sortParamsNew + ") rn from " +
//                                    "(select * from " +
//                                    "(select *,com(" + featureByImageId + ",feature) as similarity " +
//                                    "from person_table) temp1 " +
//                                    "where similarity >= ? " +
//                                    "and startTime = ?" +
//                                    "and endTime = ?" +
//                                    "and ipc in ({0})" +
//                                    ") " + "temp2 ) " + "temp3" +
//                                    "where temp3.rn >= ? and temp3.rn <= ? ";
                            jdbcUtil.executeQuery(selecttest, params, new JDBCUtil.QueryCallback() {
                                @Override
                                public void process(ResultSet rs) throws Exception {
                                    //rowkey
                                    String imageId = rs.getString(1);
                                    //设备id
                                    String ipcId = rs.getString(2);
                                    //相似度
                                    Float similarity = rs.getFloat(3);
                                    //时间戳
                                    Long timeStamp = rs.getLong(4);

                                    capturedPicture = new CapturedPicture();
                                    capturedPicture.setId(imageId);
                                    capturedPicture.setIpcId(ipcId);
                                    capturedPicture.setTimeStamp(timeStamp);
                                    capturedPicture.setSimilarity(similarity);
                                }
                            });
                            capturedPictureList = new ArrayList<>();
                            capturedPictureList.add(capturedPicture);
                            /*boolean flag = dynamicPhotoService.insertSearchRes(searchId, capturedPictureList, insertType);
                            if (flag) {
                                LOG.info("The search history of: [" + searchId + "] saved successful");
                            } else {
                                LOG.error("The search history of: [" + searchId + "] saved failure");
                            }*/
                            searchResult.setSearchId(searchId);
                            searchResult.setPictures(capturedPictureList);
                        } else {
                            //无图无imageId,通过其他参数查询
                            //添加过滤条件
                            String sortParamsNew = ListUtils.getOrderStringBySort1(sortParams);
                            //查询需要传入的参数
                            //阈值/排序参数/分页查询开始行/分页查询数
                            Object[] params = {threshold, startTime, endTime, offset, count};
                            String compareByOthers = "select temp1.* from " +
                                    "(select *,row_number() over(order by " + sortParamsNew + ") rn " +
                                    "from person_table) temp1 " +
                                    "where similarity >= ? " +
                                    "and startTime = ?" +
                                    "and endTime = ?" +
                                    "and ipc in ({0})" +
                                    ") " +
                                    "where temp3.rn >= ? and temp3.rn <= ? ";

                        }
                    }
                }

            }
        } else {
                LOG.error("search parameter option is null");
                searchResult.setSearchId(null);
                searchResult.setPictures(null);
                searchResult.setTotal(0);
            }
            return searchResult;
        }

        public static void main(String[] args){

        RealTimeCompareBySparkSQL realTimeCompareBySparkSQL = new RealTimeCompareBySparkSQL();

        SearchOption option = new SearchOption();
        option.setSortParams("ipc Desc,rowkey asc");

        //option.setImage("");
        option.setThreshold(60);
        option.setSearchType(SearchType.PERSON);


        }
    }
