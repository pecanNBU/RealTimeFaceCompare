package com.hzgc.ftpserver.kafka.ftp;

import com.hzgc.ftpserver.kafka.producer.ProducerOverFtp;
import com.hzgc.ftpserver.local.LocalIODataConnection;
import com.hzgc.ftpserver.local.LocalSTOR;
import com.hzgc.ftpserver.util.FtpUtil;
import com.hzgc.jni.FaceFunction;
import com.hzgc.jni.NativeFunction;
import com.hzgc.rocketmq.util.RocketMQProducer;
import com.hzgc.util.IOUtil;
import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.*;
import org.apache.ftpserver.impl.*;
import org.apache.ftpserver.util.IoUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.rocketmq.client.producer.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Map;

public class KafkaSTOR extends AbstractCommand {
    private final Logger LOG = LoggerFactory.getLogger(KafkaSTOR.class);

    public void execute(final FtpIoSession session,
                        final FtpServerContext context, final FtpRequest request)
            throws IOException, FtpException {
        KafkaFtpServerContext kafkaContext = null;
        if (context instanceof KafkaFtpServerContext) {
            kafkaContext = (KafkaFtpServerContext) context;
        }
        try {
            // argument check
            String fileName = request.getArgument();
            if (fileName == null) {
                session
                        .write(LocalizedFtpReply
                                .translate(
                                        session,
                                        request,
                                        kafkaContext,
                                        FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
                                        "STOR", null));
                return;
            }

            DataConnectionFactory connFactory = null;
            try {
                connFactory = session.getDataConnection();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (connFactory != null) {
                InetAddress address = ((ServerDataConnectionFactory) connFactory)
                        .getInetAddress();
                if (address == null) {
                    session.write(new DefaultFtpReply(
                            FtpReply.REPLY_503_BAD_SEQUENCE_OF_COMMANDS,
                            "PORT or PASV must be issued first"));
                    return;
                }
            }

            // get filename
            FtpFile file = null;
            try {
                file = session.getFileSystemView().getFile(fileName);
            } catch (Exception ex) {
                LOG.info("Exception getting file object", ex);
            }
            if (file == null) {
                session.write(LocalizedFtpReply.translate(session, request, kafkaContext,
                        FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN,
                        "STOR.invalid", fileName));
                return;
            }
            fileName = file.getAbsolutePath();
            // get permission
            if (!file.isWritable()) {
                session.write(LocalizedFtpReply.translate(session, request, kafkaContext,
                        FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN,
                        "STOR.permission", fileName));
                return;
            }

            // get data connection
            session.write(
                    LocalizedFtpReply.translate(session, request, kafkaContext,
                            FtpReply.REPLY_150_FILE_STATUS_OKAY, "STOR",
                            fileName)).awaitUninterruptibly(10000);

            LocalIODataConnection dataConnection;
            try {
                IODataConnectionFactory customConnFactory = (IODataConnectionFactory) session.getDataConnection();
                dataConnection = new LocalIODataConnection(customConnFactory.createDataSocket(), customConnFactory.getSession(), customConnFactory);
            } catch (Exception e) {
                LOG.info("Exception getting the input data stream", e);
                session.write(LocalizedFtpReply.translate(session, request, kafkaContext,
                        FtpReply.REPLY_425_CANT_OPEN_DATA_CONNECTION, "STOR",
                        fileName));
                return;
            }

            // transfer data
            boolean failure = false;
            ByteArrayOutputStream value = null;
            InputStream is = null;
            try {
                is = dataConnection.getDataInputStream();
                value = FtpUtil.inputStreamCacher(is);
                byte[] photBytes = value.toByteArray();
                String key = FtpUtil.transformNameToKey(fileName);
                ProducerOverFtp kafkaProducer = kafkaContext.getProducerOverFtp();
                RocketMQProducer rocketMQProducer = kafkaContext.getProducerRocketMQ();
                //parsing JSON files
                if (key.contains("unknown")) {
                    LOG.error(key + ": unknown ipcID, Not send to Kafka!");
                } else {
                    if (file.getName().contains(".json")) {
                        kafkaProducer.sendKafkaMessage(ProducerOverFtp.getJson(), key, photBytes);

                    } else if (fileName.contains(".jpg")) {
                        //it is picture
                        if (FtpUtil.pickPicture(fileName) == 0) {
                            kafkaProducer.sendKafkaMessage(ProducerOverFtp.getPicture(), key, photBytes);
                        } else if (FtpUtil.pickPicture(fileName) > 0) {
                            int faceNum = FtpUtil.pickPicture(fileName);
                            String faceKey = FtpUtil.faceKey(faceNum, key);
                            kafkaProducer.sendKafkaMessage(ProducerOverFtp.getFace(), faceKey, photBytes);
                            Map<String, String> parseKey = FtpUtil.getRowKeyMessage(faceKey);
                            // TODO: 2017-10-12
                            SendResult tempResult = rocketMQProducer.
                                    send(parseKey.get("ipcID"), parseKey.get("mqkey"), photBytes);

                            rocketMQProducer.send(rocketMQProducer.getMessTopic(), parseKey.get("ipcID"),
                                    parseKey.get("mqkey"), tempResult.getOffsetMsgId().getBytes(), null);
                            float[] feature = FaceFunction.featureExtract(photBytes);
                            if (feature != null && feature.length == 512) {
                                kafkaProducer.sendKafkaMessage(ProducerOverFtp.getFEATURE(),
                                        faceKey,
                                        Bytes.toBytes(FaceFunction.floatArray2string(feature)));
                            }
                        } else {
                            LOG.info("Contains illegal file[" + file.getName() + "], write to local default");
                        }
                    }
                }
                // attempt to close the output stream so that errors in
                // closing it will return an error to the client (FTPSERVER-119)

                LOG.info("File uploaded {}", fileName);

                // notify the statistics component
                ServerFtpStatistics ftpStat = (ServerFtpStatistics) kafkaContext
                        .getFtpStatistics();
                ftpStat.setUpload(session, file, Long.MAX_VALUE);

            } catch (SocketException ex) {
                LOG.info("Socket exception during data transfer", ex);
                failure = true;
                session.write(LocalizedFtpReply.translate(session, request, kafkaContext,
                        FtpReply.REPLY_426_CONNECTION_CLOSED_TRANSFER_ABORTED,
                        "STOR", fileName));
            } catch (IOException ex) {
                LOG.info("IOException during data transfer", ex);
                failure = true;
                session
                        .write(LocalizedFtpReply
                                .translate(
                                        session,
                                        request,
                                        kafkaContext,
                                        FtpReply.REPLY_551_REQUESTED_ACTION_ABORTED_PAGE_TYPE_UNKNOWN,
                                        "STOR", fileName));
            } finally {
                IOUtil.closeStream(is);
                IOUtil.closeStream(value);
            }

            // if data transfer ok - send transfer complete message
            if (!failure) {
                session.write(LocalizedFtpReply.translate(session, request, kafkaContext,
                        FtpReply.REPLY_226_CLOSING_DATA_CONNECTION, "STOR",
                        fileName));

            }
        } finally {
            session.resetState();
            session.getDataConnection().closeDataConnection();
        }
    }
}
