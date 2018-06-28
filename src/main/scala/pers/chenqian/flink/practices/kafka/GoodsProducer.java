package pers.chenqian.flink.practices.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class GoodsProducer {


    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("acks", "all");
        //props.put("retries", 0);
        //props.put("batch.size", 16384);
        //props.put("linger.ms", 1);
        //props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "mytopic";
        Producer<String, String> procuder = new KafkaProducer<String,String>(props);
        String basicText = "1:1000x:27.90:100120:93412300000:1:0:";
        int sumUid = 0;

        for (int i = 1; i < 10; i++) {
            String text = basicText.replaceFirst("x", String.valueOf(i)) + System.currentTimeMillis();
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, text);
            procuder.send(msg);
            sumUid += i;
            Thread.sleep(10L);
        }

        ProducerRecord<String, String> tooLateMsg = new ProducerRecord<String, String>
                (topic, "1:11004:1004:0:0:0:0:" + (System.currentTimeMillis() - 111117000));
        procuder.send(tooLateMsg);
        sumUid += 11004;
        System.out.println("sumUid:" + sumUid);

        //列出topic的相关信息
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>() ;
        partitions = procuder.partitionsFor(topic);
//        for(PartitionInfo p:partitions)
//        {
//            System.out.println(p);
//        }

        System.out.println("send message over.");
        procuder.close(100, TimeUnit.MILLISECONDS);
    }

}
