//package com.bdilab.flinketl.examples.demo;
//
//import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
//import org.apache.kafka.clients.producer.ProducerRecord;
//
//import java.io.ByteArrayOutputStream;
//import java.io.ObjectOutputStream;
//
//import static com.ververica.flinktraining.exercises.datastream_scala.connect.util.BeanUtils.ObjectToBytes;
//
///**
// * @description:
// * @author: ljw
// * @time: 2021/9/23 19:41
// */
//
//public class ObjSerializationSchema implements KafkaSerializationSchema<TaxiRide>{
//
//    private String topic;
//
//    public ObjSerializationSchema(String topic) {
//        super();
//        this.topic = topic;
//    }
//
//    @Override
//    public ProducerRecord<byte[], byte[]> serialize(TaxiRide obj, Long timestamp) {
//        byte[] bytes1 = null;
//        bytes1 = ObjectToBytes(obj);
//        ObjectOutputStream outputStream = null;
//        ByteArrayOutputStream arrayOutputStream = null;
//        try {
//            arrayOutputStream = new ByteArrayOutputStream();
//            outputStream = new ObjectOutputStream(arrayOutputStream);
//            outputStream.writeObject(obj);
//            byte[] bytes = arrayOutputStream.toByteArray();
//            bytes1 = bytes;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return new ProducerRecord<byte[], byte[]>(topic, bytes1);
//    }
//
//}
