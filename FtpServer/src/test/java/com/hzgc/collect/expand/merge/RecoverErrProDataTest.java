package com.hzgc.collect.expand.merge;

import com.hzgc.collect.expand.conf.CommonConf;
import com.hzgc.collect.expand.log.LogEvent;
import com.hzgc.collect.expand.processer.FaceObject;
import com.hzgc.collect.expand.processer.KafkaProducer;
import com.hzgc.collect.expand.util.JSONHelper;
import com.hzgc.jni.NativeFunction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RecoverErrProDataTest {

    private MergeUtil mergeUtil = new MergeUtil();

    private CommonConf commonConf = new CommonConf();
    private String processLogDir = commonConf.getProcessLogDir();
    private String mergeErrLogDir = commonConf.getMergeLogDir() + "/error";

    private String SUFFIX = ".log";

    private String ftpBackupDir = "/home/test/backup/ftp";
    private String ftpDir = "/home/test/ftp";

    //初始化算法
    static {
        NativeFunction.init();
    }


    /**
     * 辅助工具
     * copy file
     */
    public void copyFile(String sourceFile, String destinationFile) throws IOException {
        if (sourceFile != null && destinationFile != null
                && !Objects.equals(sourceFile, "")
                && !Objects.equals(destinationFile, "")) {
            if (new File(sourceFile).exists() && new File(sourceFile).isFile()) {
                File destinationFolder = new File(destinationFile).getParentFile();
                if (!destinationFolder.exists()) {
                    destinationFolder.mkdirs();
                }
                //拷贝文件。REPLACE_EXISTING: 如果目标文件存在，则替换。如果不存在，则移动。
                Files.copy(Paths.get(sourceFile), Paths.get(destinationFile),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    /**
     * 辅助工具
     * copy Dir
     */
    private void copyDir(String fromPath, String toPath) throws IOException {
        copyDir(new File(fromPath), new File (toPath));
    }

    /**
     * 辅助工具
     * copy Dir
     */
    private void copyDir(File fromPath, File toPath) throws IOException {
        if (fromPath.exists() && fromPath.isDirectory()){
            if (!toPath.exists()){
                toPath.mkdirs();
            }
            File[] files = fromPath.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()){
                    String fromDir = files[i].getAbsolutePath();
                    String toDir = toPath.getAbsolutePath() + File.separator + files[i].getName();
                    copyDir(fromDir,toDir);
                    continue;
                }
                String fromFile = fromPath.getAbsolutePath() + File.separator + files[i].getName();
                String toFile = toPath.getAbsolutePath() + File.separator + files[i].getName();
                copyFile(fromFile, toFile);
            }

        }
    }

    /**
     * 测试开始前的动作
     * @throws IOException
     */
    @Before
    public void before() throws IOException {
        mergeUtil.deleteFile(ftpDir);
        copyDir(ftpBackupDir,ftpDir);
    }

    /**
     * 局部功能
     * 发送kafka测试
     */
    @Test
    public void testSendToKafka() {
        MergeUtil mergeUtil = new MergeUtil();

        String ftpdataDir = commonConf.getFtpdataDir();
        System.out.println("processDir:" + processLogDir);
        System.out.println("mergeErrLogDir:" + mergeErrLogDir);
        System.out.println("ftpdataDir:" + ftpdataDir);

        List<String> allErrorDir = mergeUtil.listAllErrorLogAbsPath(processLogDir);
        for (String errFile : allErrorDir) {
            String successErrFile = mergeUtil.getSuccessFilePath(errFile);
            String mergeErrFile = mergeUtil.getMergeFilePath(errFile);
            mergeUtil.lockAndMove(errFile, mergeErrFile);
            mergeUtil.copyFile(mergeErrFile, successErrFile);
        }

        List<String> errFilePaths = mergeUtil.listAllFileAbsPath(mergeErrLogDir);
        if (errFilePaths != null && errFilePaths.size() != 0) {
            for (String errorFilePath : errFilePaths) {
                List<String> errorRows = mergeUtil.getAllContentFromFile(errorFilePath);
                if (errorRows != null && errorRows.size() != 0) {
                    for (String row : errorRows) {
                        System.out.println("****************************start scanning...****************************");

                        LogEvent event = JSONHelper.toObject(row, LogEvent.class);

                        long count = event.getCount();
                        String ftpUrl = event.getPath();
                        System.out.println("****************************ftpUrl:" + ftpUrl + "****************************");
                        System.out.println("****************************get faceObject...****************************");
                        //根据路径取得对应的图片，并提取特征，封装成FaceObject，发送Kafka
                        FaceObject faceObject = GetFaceObject.getFaceObject(row, ftpdataDir);
                        System.out.println("****************************faceObject:" + faceObject + "****************************");
                        if (faceObject != null) {
                            SendDataToKafka sendDataToKafka = SendDataToKafka.getSendDataToKafka();
                            sendDataToKafka.sendKafkaMessage(KafkaProducer.getFEATURE(), ftpUrl, faceObject);
                            boolean success = sendDataToKafka.isSuccessToKafka();
                            System.out.println("****************************test log!****************************");
                            //若发送kafka不成功，将错误日志写入/merge/error/下一个新的errorN-NEW日志中
                            String mergeErrFileNew = errorFilePath.replace(SUFFIX, "") + "-N" + SUFFIX;
                            if (!success) {
                                System.out.println("****************************Send the count " + count +
                                        "message to kafka failed! Rewrite to new merge error file!" +"****************************");
                                mergeUtil.writeMergeFile(event, mergeErrFileNew);
                            } else {
                                System.out.println("****************************Send the count " + count +
                                        "message to kafka successfully!" +"****************************");
                            }
                        }
                    }
                }
                mergeUtil.deleteFile(errorFilePath);
            }
        } else { //若merge/error目录下无日志
            System.out.println("****************************Nothing in " + mergeErrLogDir + "****************************");
        }

    }


    /**
     * 局部功能
     * 测试获取process下错误日志的锁，并移动到success和merge的部分。
     */
    @Test
    public void testLockAndMove() throws IOException {

        System.out.println(("************************************" +
                "testLockAndMove：将process目录下所有能获取到锁的error日志，移动到success和merge" +
                "************************************"));
        List<String> allErrorDir = mergeUtil.listAllErrorLogAbsPath(processLogDir);
        for (int i = 0; i < allErrorDir.size(); i++) {
            //获取每个error.log需要移动到的success和merge目录下的路径
            String successErrFile = mergeUtil.getSuccessFilePath(allErrorDir.get(i));
            String mergeErrFile = mergeUtil.getMergeFilePath(allErrorDir.get(i));
            System.out.println("*************第 " + i + " 个error.log对应的success备份路径和merge处理路径：**************");
            System.out.println(allErrorDir.get(i));
            System.out.println(successErrFile);
            System.out.println(mergeErrFile);
            //移动前，每个errorFile中的内容
            List<String> errContentBefore = mergeUtil.getAllContentFromFile(allErrorDir.get(i));

            //移动到merge后，拷贝一份到success
            mergeUtil.lockAndMove(allErrorDir.get(i), mergeErrFile); //其中包括判断锁是否存在
            mergeUtil.copyFile(mergeErrFile, successErrFile);

            //移动后，每个errorFile中的内容
            List<String> errContentAfter = mergeUtil.getAllContentFromFile(allErrorDir.get(i));
            Assert.assertEquals("移动后，原error日志不为空！", 0, errContentAfter.size());
            List<String> errContentMerge = mergeUtil.getAllContentFromFile(mergeErrFile);
            Assert.assertEquals("移动后，merge/error日志与原日志内容不相同！", errContentBefore, errContentMerge);
            List<String> errContentSuc = mergeUtil.getAllContentFromFile(successErrFile);
            Assert.assertEquals("移动后，success/error日志与原日志内容不相同！", errContentBefore, errContentSuc);
            System.out.println(errContentBefore);
            System.out.println(errContentAfter);
            System.out.println(errContentMerge);
            System.out.println(errContentSuc);

        }
    }


    /**
     * 局部功能
     * 测试处理merge/error的错误日志部分
     * 假设每个error.log的前两条发送kafka失败，看是否能够写入到merge/error下的新日志中。
     * 不包含发送kafka
     */
    @Test
    public void testDealMergeError() throws IOException {

        //prepare
        String processErrDir = processLogDir + File.separator + "p-0" + File.separator + "error";
        copyDir(processErrDir, mergeErrLogDir);

        System.out.println(("************************************" +
                "testDealMergeError：测试处理merge/error的错误日志部分。" + "\n" +
                "假设每个error.log的前两条发送kafka失败，看是否能够写入到merge/error下的新日志中。" +
                "************************************"));

        List<String> errFilePaths = mergeUtil.listAllFileAbsPath(mergeErrLogDir);
        if (errFilePaths != null && errFilePaths.size() != 0) {
            //对于每一个error.log
            for (String errorFilePath : errFilePaths) {
                String mergeErrFileNew = errorFilePath.replace(SUFFIX, "") + "-N" + SUFFIX;
                System.out.println("*****************************每个errorFile对应的新的errorFile-N：*****************************");
                System.out.println(errorFilePath);
                System.out.println(mergeErrFileNew);
                List<String> errorRows = mergeUtil.getAllContentFromFile(errorFilePath);
                if (errorRows != null && errorRows.size() != 0) {
                    for (int i = 0; i < errorRows.size(); i++) {
                        System.out.println(errorRows.get(i));
                        LogEvent event = JSONHelper.toObject(errorRows.get(i), LogEvent.class);
                        //测试时，假设每个error.log的前两条发送kafka失败
                        boolean success;
                        if (i < 2) {
                            success = false;
                        } else {
                            success = true;
                        }

                        //发送失败的前两条需要写入新的merge/error-N.log，其他的均发送成功
                        if (!success) {
                            mergeUtil.writeMergeFile(event, mergeErrFileNew);
                        }
                    }

                    //原本error.log中的前两行，放入List中
                    List<String> errorTwo = new ArrayList<>();
                    errorTwo.add(errorRows.get(0));
                    errorTwo.add(errorRows.get(1));
                    System.out.println("每个error.log发送失败的前两条:" + errorTwo);

                    //error.log的前两行新写入的error-N，放入List中
                    List<String> mergeErrFileNewList = Files.readAllLines(Paths.get(mergeErrFileNew));
                    System.out.println("新写入的error-N:" + mergeErrFileNewList);

                    Assert.assertEquals("每个error.log发送失败的前两条没有全部写入到新的merge/error！", errorTwo.size(), mergeErrFileNewList.size());

                    //比较新写入的mergeErrFileNew，是否和error.log中的前两行相同
                    Assert.assertEquals("新写入的mergeErrFileNew，和原error.log中的前两行不相同！", errorTwo, mergeErrFileNewList);

                }
                mergeUtil.deleteFile(errorFilePath); //删除已处理过的error日志
                Assert.assertTrue("已处理过的error日志未删除！",!new File(errorFilePath).exists());
            }

        } else { //若merge/error目录下无日志
            System.out.println("Nothing in " + mergeErrLogDir);
        }
    }


}
