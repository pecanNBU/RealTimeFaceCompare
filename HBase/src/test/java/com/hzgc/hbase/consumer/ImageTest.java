package com.hzgc.hbase.consumer;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Administrator on 2017-8-18.
 */
public class ImageTest implements Serializable {
    private static Logger LOG = Logger.getLogger(ImageTest.class);

    @Test
    public void timeTrans() {
        long a = Long.valueOf("3616444613829605421");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String aa = formatter.format(a);
        System.out.println(aa);
    }

    //@Test
    /*public void readImageHBase() {
        String rowKey = "f4a1f5f38f2a41a7a71997fa31f84fbd";
        byte[] image = GetPicFromHBase.getPicFromHBase(rowKey);
        if (null != image && image.length > 0) {
            ImageHander.byte2image(image, "E:\\spark\\RealTimeFaceCompare\\HBase\\src\\test\\java\\com\\hzgc\\hbase\\consumer\\123.jpg");
        } else {
            LOG.error("image is null");
        }
    }*/

    @Test
    public void transformNameToKey() {
       /* String s = "17130NCY0HZ0004-0_00000000000000_170801160015_0000001111_00";
        String ss = FtpUtil.getRowKeyMessage(s).get("time");
        System.out.println(ss);
        long timestamp = Long.valueOf(ss);
        System.out.println("ssssssssss" + timestamp);
        Date date = new Date(timestamp);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String sss = dateFormat.format(date);
        System.out.println(sss);*/

    }

    @Test
    public void getProcessNumber() {
        int availProcessors = Runtime.getRuntime().availableProcessors();
        System.out.println("avail processors count: " + availProcessors);
    }
}
