package pers.chenqian.flink.practices

import java.net.{InetAddress, InetSocketAddress}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
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
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    import org.apache.flink.api.scala._

    val text = env.addSource(new FlinkKafkaConsumer011[String]("mytopic", new SimpleStringSchema(), properties))

    val mappedDS = text.map(mapToEsKV(_))

    //addToEs(mappedDS)
//    window(mappedDS)
//    aggregate(mappedDS)
    reduce(mappedDS)

    env.execute("Socket Window WordCount")
  }





}
