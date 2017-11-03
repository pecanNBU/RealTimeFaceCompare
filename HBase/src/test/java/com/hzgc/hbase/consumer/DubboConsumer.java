package com.hzgc.hbase.consumer;

import com.hzgc.dubbo.dynamicrepo.*;
import com.hzgc.hbase.dynamicrepo.CapturePictureSearchServiceImpl;
import com.hzgc.hbase.staticrepo.ElasticSearchHelper;
import com.hzgc.jni.NativeFunction;

import java.util.ArrayList;
import java.util.List;

public class DubboConsumer {
    static {
        ElasticSearchHelper.getEsClient();
    }

    public static void main(String[] args) {
        //测试常规服务
      /*  ClassPathXmlApplicationContext context =
                new ClassPathXmlApplicationContext("consumer.xml");
        context.start();
        System.out.println("consumer start");
        CapturePictureSearchService capturePictureSearchService = (CapturePictureSearchService) context.getBean("CapturePictureSearchService");
        System.out.println("consumer");*/

        NativeFunction.init();
        CapturePictureSearchService capturePictureSearchService = new CapturePictureSearchServiceImpl();
       /* CapturedPicture capturedPicture=capturePictureSearchService.getCaptureMessage("3B0383FPAG00883_170914101839_0000006269_01",2);
        System.out.println(capturedPicture);*/

        /*long time = System.currentTimeMillis();
        SearchResult searchResult = capturePictureSearchService.getSearchResult("f1ec5d3d993d437faef70fc6d03547a1", 100, 10, "-timeStamp");
        if (null != searchResult) {
            int total = searchResult.getTotal();
            System.out.println(searchResult);
            List<CapturedPicture> capturedPictures = searchResult.getPictures();
            for (CapturedPicture capturedPictures1 : capturedPictures) {
                System.out.println("capture:" + capturedPictures1);
            }
            System.out.println(total);
        }
        System.out.println(System.currentTimeMillis() - time);*/

       /* List<String> imgList = new ArrayList<>();
        imgList.add("3B0383FPAG00883_170919154204_0000006451_01");
        imgList.add("2L04129PAU01933_170919163212_0000006238_01");
        imgList.add("2L04129PAU01933_170919162826_0000004328_01");
        List<CapturedPicture> capturedPictureList = new ArrayList<>();
        capturedPictureList = capturePictureSearchService.getBatchCaptureMessage(imgList, 2);
        for (CapturedPicture capturedPicture : capturedPictureList) {
            System.out.println(capturedPicture);
        }*/


        List<Long> time2 = new ArrayList<Long>();
        for (int i = 0; i < 1; i++) {
            SearchOption option = new SearchOption();
            option.setSearchType(SearchType.PERSON);
            List<String> deviceIds = new ArrayList<>();
            deviceIds.add("0");
            deviceIds.add("1");
            /*deviceIds.add("2L04129PAU01933");
            deviceIds.add("DS-2CD2T20FD-I320160122AACH571485690");*/
            //option.setDeviceIds(deviceIds);
            //option.setImageId("3B0383FPAG00883_170905202728_0000021911_01");
            option.setImageId("device-test-5-360302197610192541");
            option.setThreshold(60.00f);
            option.setSortParams("-similarity");
            option.setOffset(0);
            option.setCount(20);
            /*byte[] image = ImageHander.image2Byte("E:\\spark\\RealTimeFaceCompare\\HBase\\src\\test\\java\\com\\hzgc\\hbase\\consumer\\22.jpg");
            option.setImage(image);*/
            long startTime = System.currentTimeMillis();
            SearchResult searchResult = capturePictureSearchService.search(option);
            /*List<CapturedPicture> capturedPictures = searchResult.getPictures();
            for (CapturedPicture capturedPictures1 : capturedPictures) {
                System.out.println("capture:" + capturedPictures1);
            }*/
            long searchTime = System.currentTimeMillis() - startTime;
            System.out.println("查询时间：" + searchTime);
            time2.add(searchTime);
            int total = searchResult.getTotal();
            System.out.println(searchResult);
            System.out.println("数据总量：" + total);
            System.out.println(time2);
            System.out.println("平均查询时间：" + time2.stream().mapToLong((x) -> x).summaryStatistics().getAverage());
        }
    }
}
