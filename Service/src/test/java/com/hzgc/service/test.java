package com.hzgc.service;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created by Administrator on 2018-1-3.
 */
public class test {
    @Test
    public void dateTrans() {
        String timestamp = "2018-01-03 15:57:26";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        sdf1.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            java.util.Date timeStampDate = sdf.parse(timestamp);
            timestamp = sdf1.format(timeStampDate);
            System.out.println(timestamp);
            timestamp=sdf1.format(timeStampDate);
            System.out.println(timestamp);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
