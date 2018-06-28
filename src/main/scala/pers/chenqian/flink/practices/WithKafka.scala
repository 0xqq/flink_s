package pers.chenqian.flink.practices

import java.net.{InetAddress, InetSocketAddress}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import pers.chenqian.flink.practices.basic.{WithKafkaBasic, WithKafkaFailed}
import pers.chenqian.flink.practices.constants.Key

import scala.collection.mutable

object WithKafka extends WithKafkaBasic with WithKafkaFailed {


  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    // get the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.api.scala._

    val text = env
      .addSource(new FlinkKafkaConsumer011[String]("mytopic", new SimpleStringSchema(), properties))
      .setParallelism(1)

    val mappedDS = text.map(mapToArray(_))

    //addToEs(mappedDS)
//    addSink(mappedDS)
//    assignTimestampsAndWatermarks(mappedDS)
//    window(mappedDS)
//    windowAll(mappedDS)
//    aggregate(mappedDS)
//    reduce(mappedDS)
    sqlOnly1(env, mappedDS)
//    sqlOnly2(env, mappedDS)
//    scanAndSqlOpe(env, mappedDS)

    env.execute("Socket Window WordCount")
  }





}
