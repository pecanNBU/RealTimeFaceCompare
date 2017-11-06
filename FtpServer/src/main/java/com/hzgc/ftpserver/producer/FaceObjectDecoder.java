package com.hzgc.ftpserver.producer;


import com.hzgc.ftpserver.util.BeanUtils;

import java.util.Map;

public class FaceObjectDecoder implements org.apache.kafka.common.serialization.Deserializer<FaceObject> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public FaceObject deserialize(String topic, byte[] data) {
        return BeanUtils.bytesToObject(data);
    }

    @Override
    public void close() {

    }

   /* @Override
    public FaceObject fromBytes(byte[] bytes) {
        return BeanUtils.bytesToObject(bytes);
    }*/
}
