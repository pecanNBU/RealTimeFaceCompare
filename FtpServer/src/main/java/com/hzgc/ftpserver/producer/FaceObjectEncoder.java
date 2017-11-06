package com.hzgc.ftpserver.producer;

import com.hzgc.ftpserver.util.BeanUtils;

import java.util.Map;


public class FaceObjectEncoder implements org.apache.kafka.common.serialization.Serializer<FaceObject> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, FaceObject data) {
        return BeanUtils.objectToBytes(data);
    }

    @Override
    public void close() {

    }
}
