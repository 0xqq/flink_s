package pers.chenqian.flink.practices

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerRecord
import pers.chenqian.flink.practices.constants.Key

import scala.collection.mutable

object WithKafka {


  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val text = env.addSource(new FlinkKafkaConsumer011[String]("mytopic", new SimpleStringSchema(), properties))

    val mapped = text.map(mapToEsKV(_))

    mapped.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }


  protected def mapToEsKV(strVal: String): mutable.HashMap[String, Any] = {
    val partsArr = strVal.split(Key.KAFKA_SEP)
    val map = new mutable.HashMap[String, Any]
    try {
      val nowMs = System.currentTimeMillis()

      if (partsArr.length == 7) {
        map ++= Seq(
          Key.GOODS_ID_ -> partsArr(0).toLong, Key.USER_ID_ -> partsArr(1).toLong,
          Key.RAITING -> partsArr(2).toDouble, Key.VIEW_COUNTS_ -> partsArr(3).toInt,
          Key.STAY_MS_ -> partsArr(4).toLong, Key.IS_STAR_ -> partsArr(5).toInt,
          Key.BUY_COUNTS_ -> partsArr(6).toInt, Key.AT_TIMESTAMP -> nowMs
        )
      } else {
        println(s"strVal: $strVal 's partsArr.length:${partsArr.length} must be 7, so ignore this ConsumerRecord")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    return map
  }

}
