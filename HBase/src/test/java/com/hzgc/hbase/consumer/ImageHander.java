package com.hzgc.hbase.consumer;

import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.FileImageOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Administrator on 2017-8-8.
 */
public class ImageHander implements Serializable {
    //图片到byte数组
    public static byte[] image2Byte(String path) {
        byte[] data = null;
        FileImageInputStream input;
        try {
            input = new FileImageInputStream(new File(path));
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int numBytesRead;
            while ((numBytesRead = input.read(buf)) != -1) {
                output.write(buf, 0, numBytesRead);
            }
            data = output.toByteArray();
            output.close();
            input.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return data;
    }

    //byte数组到图片
    public static void byte2image(byte[] data, String path) {
        if (data.length < 3 || path.equals("")) return;
        try {
            FileImageOutputStream imageOutput = new FileImageOutputStream(new File(path));
            imageOutput.write(data, 0, data.length);
            imageOutput.close();
            System.out.println("Make Picture success,Please find image in " + path);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex);
            ex.printStackTrace();
        }
    }
}
