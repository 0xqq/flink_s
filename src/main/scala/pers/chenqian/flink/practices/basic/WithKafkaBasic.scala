package pers.chenqian.flink.practices.basic

import java.net.{InetAddress, InetSocketAddress}
import java.util

import org.apache.flink.api.common.accumulators.DoubleMaximum
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.table.api.{QueryConfig, Table, TableEnvironment}
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.util.Collector
import pers.chenqian.flink.practices.constants.{Idx, Key}
import pers.chenqian.flink.practices.sink.RedisExampleMapper

import scala.collection.mutable

class WithKafkaBasic {


  /**
    * 12345:100012:26.90:100000:93470000000:0:0
    */
  def mapToArray(strVal: String): Array[Double] = {
    val partsArr = strVal.split(Key.KAFKA_SEP)
    try {
      val nowMs = System.currentTimeMillis()

      if (partsArr.length == 8) {
        return partsArr.map(_.toDouble)
      } else {
        println(s"strVal: $strVal 's partsArr.length:${partsArr.length} must be 7, so ignore this ConsumerRecord")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    return new Array[Double](0)
  }


  def addToEs(mappedDS: DataStream[mutable.HashMap[String, Any]]) = {
    val config = new java.util.HashMap[String, String]
    config.put("cluster.name", "my-cluster-name")
    // This instructs the sink to emit after every element, otherwise they would be buffered
    config.put("bulk.flush.max.actions", "1")

    val transportAddresses = new java.util.ArrayList[InetSocketAddress]
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300))

    //    mappedDS.addSink(new ElasticsearchSink(config, transportAddresses, new ElasticsearchSinkFunction[String] {
    //      def createIndexRequest(element: String): IndexRequest = {
    //        val json = new java.util.HashMap[String, String]
    //        json.put("data", element)
    //
    //        return Requests.indexRequest()
    //          .index("my-index")
    //          .type("my-type")
    //          .source(json)
    //      }
    //    }))
  }

  def addSink(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    val conf = new FlinkJedisPoolConfig.Builder()
      .setDatabase(0)
      .setHost("localhost")
      .setPort(6379)
//      .setNodes(new util.HashSet[InetSocketAddress](util.Arrays.asList(
//        new InetSocketAddress("localhost", 6379)
//      )))
      .build()

    mappedDS
      .map(arr => arr(Idx.GOODS_ID).toString -> arr(Idx.USER_ID).toString)
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .maxBy(Idx.USER_ID)
      .addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))
      .setParallelism(1)
  }


  def assignTimestampsAndWatermarks(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    mappedDS
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Array[Double]](Time.milliseconds(5000L)) {
        override def extractTimestamp(element: Array[Double]): Long = element.last.toLong
      })
      .keyBy(_ (Idx.GOODS_ID))
      .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5000L)))
      .maxBy(Idx.USER_ID)
      .map(_.mkString("|"))
      .print().setParallelism(1)

  }


  def window(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    mappedDS
      .keyBy(_ (Idx.GOODS_ID))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .maxBy(Idx.USER_ID)
      .map(_.mkString("|"))
      .print().setParallelism(1)
  }

  def windowAll(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    mappedDS
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .maxBy(Idx.USER_ID)
      .map(_.mkString("|"))
      .print().setParallelism(1)
  }

  def window2(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    mappedDS
      .keyBy(_ (Idx.GOODS_ID))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .aggregate(new AggregateFunction[Array[Double], DoubleMaximum, Double] {
        @Override
        def createAccumulator(): DoubleMaximum = {
          return new DoubleMaximum()
        }

        @Override
        def add(value: Array[Double], accumulator: DoubleMaximum): DoubleMaximum = {
          accumulator.add(value(Idx.USER_ID))
          return accumulator
        }

        @Override
        def getResult(accumulator: DoubleMaximum): Double = {
          return accumulator.getLocalValue()
        }

        @Override
        def merge(a: DoubleMaximum, b: DoubleMaximum): DoubleMaximum = {
          a.merge(b)
          return a
        }
      })
      //      .map(_.mkString("|"))
      .print().setParallelism(1)

  }


  def aggregate(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    mappedDS
      .keyBy(_ (Idx.GOODS_ID))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .aggregate(
        new AggregateFunction[Array[Double], DoubleMaximum, Double] {
          @Override
          def createAccumulator(): DoubleMaximum = {
            return new DoubleMaximum()
          }
          @Override
          def add(value: Array[Double], accumulator: DoubleMaximum): DoubleMaximum = {
            accumulator.add(value(Idx.USER_ID))
            return accumulator
          }
          @Override
          def getResult(accumulator: DoubleMaximum): Double = {
            return accumulator.getLocalValue()
          }
          @Override
          def merge(a: DoubleMaximum, b: DoubleMaximum): DoubleMaximum = {
            a.merge(b)
            return a
          }
        },
        new ProcessWindowFunction[Double, Double, Double, TimeWindow] {
          override def process(key: Double, context: Context, elements: Iterable[Double], out: Collector[Double]): Unit = {
            out.collect(elements.min) //这个不影响结果，那要这个重载方法有什么用?
          }
        })
      //.map(_.mkString("|"))
      .print().setParallelism(1)
  }

  def reduce(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    mappedDS
      .keyBy(_ (Idx.GOODS_ID))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .reduce((arr1, arr2) => arr1)
      //      .map(_.mkString("|"))
      .print().setParallelism(1)
  }


  def sql(env: StreamExecutionEnvironment, mappedDS: DataStream[Array[Double]]) = {
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._

    val expreArr = Seq(ExpressionParser.parseExpression(Key.GOODS_ID_))

    // SQL query with an inlined (unregistered) table
    val table: Table = tableEnv.fromDataStream(mappedDS,
      expreArr: _*)
    //'goods_id, 'user_id, 'raiting, 'view_counts, 'stay_ms, 'is_star, 'buy_counts, 'timestamp


    tableEnv.registerTable(Key.T_GOODS_RAITING, table)

    val resTable = tableEnv.sqlQuery(s"select max(${Key.USER_ID_}) from ${Key.T_GOODS_RAITING} group by ${Key.GOODS_ID_}")

    val sink = new CsvTableSink(
      s"/Users/sunzhongqian/tmp/csv/${Key.T_GOODS_RAITING}.csv", fieldDelim = ",")
    resTable.writeToSink(sink)

  }

}
