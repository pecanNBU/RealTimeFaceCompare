package com.hzgc.ftpserver.kafka.ftp;

import com.hzgc.ftpserver.ClusterOverFtp;
import com.hzgc.ftpserver.local.LocalPropertiesUserManagerFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.log4j.Logger;

public class KafkaOverFtpServer extends ClusterOverFtp {
    private static Logger log = Logger.getLogger(KafkaOverFtpServer.class);

    @Override
    public void startFtpServer() {
        KafkaFtpServerFactory serverFactory = new KafkaFtpServerFactory();
        log.info("Create " + KafkaFtpServerFactory.class + " successful");
        ListenerFactory listenerFactory = new ListenerFactory();
        log.info("Create " + ListenerFactory.class + " successful");
        //set the port of the listener
        listenerFactory.setPort(listenerPort);
        log.info("The port for listener is " + listenerPort);
        // replace the default listener
        serverFactory.addListener("default", listenerFactory.createListener());
        log.info("Add listner, name:default, class:" + serverFactory.getListener("default").getClass());
        // set customer user manager
        LocalPropertiesUserManagerFactory userManagerFactory = new LocalPropertiesUserManagerFactory();
        serverFactory.setUserManager(userManagerFactory.createUserManager());
        log.info("Set customer user manager factory is successful, " + userManagerFactory.getClass());
        //set customer cmd factory
        KafkaCmdFactoryFactory cmdFactoryFactory = new KafkaCmdFactoryFactory();
        serverFactory.setCommandFactory(cmdFactoryFactory.createCommandFactory());
        log.info("Set customer command factory is successful, " + cmdFactoryFactory.getClass());
        //set local file system
        KafkaFileSystemFactory kafkaFileSystemFactory = new KafkaFileSystemFactory();
        serverFactory.setFileSystem(kafkaFileSystemFactory);
        log.info("Set kafka file system factory is successful, " + kafkaFileSystemFactory.getClass());
        // set connection config
        KafkaConnectionConfigFactory connectionConfigFactory = new KafkaConnectionConfigFactory();
        log.info("the maxLogins is :" + connectionConfigFactory.createConnectionConfig().getMaxLogins());
        serverFactory.setConnectionConfig(connectionConfigFactory.createConnectionConfig());
        log.info("Set user defined connection config file is successful, " + connectionConfigFactory.getClass());
        FtpServer server = serverFactory.createServer();
        try {
            server.start();
        } catch (FtpException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws Exception {
        KafkaOverFtpServer kafkaOverFtpServer = new KafkaOverFtpServer();
        kafkaOverFtpServer.loadConfig();
        kafkaOverFtpServer.startFtpServer();
    }
}
