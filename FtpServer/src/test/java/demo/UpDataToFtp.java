package demo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.hzgc.ftpserver.util.Download;
import com.hzgc.util.common.UuidUtil;

import java.io.File;
import java.util.Random;
import java.util.UUID;

/**
 * éæ哄寤虹®褰锛灏涓缁å剧åéå版瀹FTP
 */
public class UpDataToFtp {
    private static MetricRegistry metric = new MetricRegistry();
    private final static Counter counter = metric.counter("counter");

    /**
     * @param path    文件路径
     * @param loopNum 循环次数
     * @param IpcId   设备ID
     */
    public static void upDataTestFirst(String path, int loopNum, String IpcId) {
        for (int i = 0; i < loopNum; i++) {
            File file = new File(path);
            File[] tempList = file.listFiles();
            String uuidStr = UuidUtil.setUuid();
            for (int j = 0; j < (tempList != null ? tempList.length : 0); j++) {
                if (tempList[j].isFile()) {
                    String orginFileName = tempList[j].getAbsolutePath();
                    String fileName = tempList[j].getName();
                    StringBuilder filePath = new StringBuilder(uuidStr);
                    filePath.append(IpcId).append("/").append(tempList[j].getName().substring(0, 13).replaceAll("_", "/"));
                    long start = System.currentTimeMillis();
                    try {
                        Download.upLoadFromProduction("172.18.18.136", 2121, "admin", "123456", "", filePath.toString(), fileName, orginFileName);
                    } catch (Exception e) {
                        System.out.println(e.toString());
                    }
                    System.out.println("current thread is:[" + Thread.currentThread() + "], start time is:[" + start + "]," +
                            "up time is:[" + (System.currentTimeMillis() - start) + "]");
                    counter.inc();
                    System.out.println(counter.getCount());
                }
            }
        }
        System.out.println("上传的文件数量：" + counter.getCount());
    }
    public static void upDataTestEnd(String path, int loopNum, String IpcId) {
        for (int i = 0; i < loopNum; i++) {
            File file = new File(path);
            File[] tempList = file.listFiles();
            String uuidStr = UuidUtil.setUuid();
            for (int j = 0; j < (tempList != null ? tempList.length : 0); j++) {
                if (tempList[j].isFile()) {
                    String orginFileName = tempList[j].getAbsolutePath();
                    String fileName = tempList[j].getName();
                    StringBuilder filePath = new StringBuilder();
                    filePath.append(IpcId).append("/").append(tempList[j].getName().substring(0, 13).replaceAll("_", "/")).append(uuidStr);
                    long start = System.currentTimeMillis();
                    try {
                        Download.upLoadFromProduction("172.18.18.136", 2121, "admin", "123456", "", filePath.toString(), fileName, orginFileName);
                    } catch (Exception e) {
                        System.out.println(e.toString());
                    }
                    System.out.println("current thread is:[" + Thread.currentThread() + "], start time is:[" + start + "]," +
                            "up time is:[" + (System.currentTimeMillis() - start) + "]");
                    counter.inc();
                    System.out.println(counter.getCount());
                }
            }
        }
        System.out.println("上传的文件数量：" + counter.getCount());
    }
}
