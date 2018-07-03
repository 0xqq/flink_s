package pers.chenqian.flink.practices

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import pers.chenqian.flink.practices.basic.{WithKafkaBasic, WithKafkaFailed}

object WithKafka extends WithKafkaBasic with WithKafkaFailed {




  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    // get the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置流的时间特征
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.api.scala._

    val text = env
      .addSource(new FlinkKafkaConsumer011[String]("mytopic", new SimpleStringSchema(), properties))

    val mappedDS = text.map(mapToArray(_))

    //addToEs(mappedDS)
//    addSink(mappedDS)
//    assignTimestampsAndWatermarks(mappedDS)
//    coGroup(env, mappedDS)
//    iterate1(env, mappedDS)
//    window(mappedDS)
//    windowAll(mappedDS)
//    aggregate(mappedDS)
//    reduce(mappedDS)
//    sqlOnly(env, mappedDS)
//    scanAndSqlOpe(env, mappedDS)
//    split1(env, mappedDS) //??
//    split2(env, mappedDS) //??

    env.execute("Socket Window WordCount")
  }





}
