package pers.chenqian.flink.practices.basic

import java.net.{InetAddress, InetSocketAddress}
import java.util

import org.apache.flink.api.common.accumulators.DoubleMaximum
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.table.api._
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.util.Collector
import pers.chenqian.flink.practices.constants.{Idx, Key}
import pers.chenqian.flink.practices.entities.GrVo
import pers.chenqian.flink.practices.sink.RedisExampleMapper

import _root_.scala.collection.mutable


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
    val config = new util.HashMap[String, String]
    config.put("cluster.name", "my-cluster-name")
    // This instructs the sink to emit after every element, otherwise they would be buffered
    config.put("bulk.flush.max.actions", "1")

    val transportAddresses = new util.ArrayList[InetSocketAddress]
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
      .print()

  }


  def window(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    mappedDS
      .keyBy(_ (Idx.GOODS_ID))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .maxBy(Idx.USER_ID)
      .map(_.mkString("|"))
      .print()
  }

  def windowAll(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    mappedDS
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .maxBy(Idx.USER_ID)
      .map(_.mkString("|"))
      .print()
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
      .print()

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
      .print()
  }

  def reduce(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    mappedDS
      .keyBy(_ (Idx.GOODS_ID))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .reduce((arr1, arr2) => arr1)
      //      .map(_.mkString("|"))
      .print()
  }


  def sqlOnly1(env: StreamExecutionEnvironment, mappedDS: DataStream[Array[Double]]) = {
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    import org.apache.flink.api.scala._

    val expreArr = Seq(Key.GOODS_ID_, Key.USER_ID_, Key.RAITING,
      Key.VIEW_COUNTS_, Key.STAY_MS_, Key.IS_STAR_, Key.BUY_COUNTS_, Key.TIMESTAMP)

    val rowDS = mappedDS
      .map(arr => GrVo.fromArray(arr))
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .maxBy(Idx.USER_ID)

    // SQL query with an inlined (unregistered) table
//    val table: Table = tableEnv.fromDataStream(rowDS)
//    tableEnv.registerTable(Key.T_GOODS_RAITING, table)

    tableEnv.registerDataStream(Key.T_GOODS_RAITING, rowDS)

    val sql = s"select goodsId, sum(userId) as sumUid from ${Key.T_GOODS_RAITING} group by goodsId"
//  val sql = s"select goodsId, userId from ${Key.T_GOODS_RAITING}"
    val resTable = tableEnv.sqlQuery(sql)

//    val sql = s"insert into t_csv select max(userId) as maxUid from ${Key.T_GOODS_RAITING} group by goodsId"
//    tableEnv.sqlUpdate(sql)

    val retractStream = tableEnv.toRetractStream[(Double, Double)](resTable)

    val csvPath = s"/Users/sunzhongqian/tmp/csv/${Key.T_GOODS_RAITING}/${System.currentTimeMillis()}/"
    val asd = retractStream
      .filter(_._1)
      .map(tp => tp._2)
      .writeAsText(csvPath, WriteMode.OVERWRITE)

  }

  def sqlOnly2(env: StreamExecutionEnvironment, mappedDS: DataStream[Array[Double]]) = {
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    import org.apache.flink.api.scala._

    val expreArr = Seq(Key.GOODS_ID_, Key.USER_ID_, Key.RAITING,
      Key.VIEW_COUNTS_, Key.STAY_MS_, Key.IS_STAR_, Key.BUY_COUNTS_, Key.TIMESTAMP)

    val rowDS = mappedDS.map(arr => GrVo.fromArray(arr))

    // SQL query with an inlined (unregistered) table
    val table: Table = tableEnv.fromDataStream(rowDS)

    tableEnv.registerTable(Key.T_GOODS_RAITING, table)

//    val sql = s"select max(userId) as maxUid from ${Key.T_GOODS_RAITING} group by goodsId"
//    val sql = s"select goodsId, userId from ${Key.T_GOODS_RAITING}"
//    val resTable = tableEnv.sqlQuery(sql)

    val csvPath = s"/Users/sunzhongqian/tmp/csv/${Key.T_GOODS_RAITING}/${System.currentTimeMillis()}/"
    val csvSink = new CsvTableSink(csvPath, ",", 1, WriteMode.OVERWRITE)
    val fieldNames: Array[String] = Array("maxUid")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.DOUBLE)
    tableEnv.registerTableSink("t_csv", fieldNames, fieldTypes, csvSink)

    val sql = s"insert into t_csv select max(userId) as maxUid from ${Key.T_GOODS_RAITING} group by goodsId"
    tableEnv.sqlUpdate(sql)

//    val csvPath = s"/Users/sunzhongqian/tmp/csv/${Key.T_GOODS_RAITING}/${System.currentTimeMillis()}/"
//    val sink = new CsvTableSink(csvPath, fieldDelim = ",")
//    resTable.writeToSink(sink)

  }


  def scanAndSqlOpe(env: StreamExecutionEnvironment, mappedDS: DataStream[Array[Double]]) = {
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._

    val rowDS = mappedDS.map(arr => GrVo.fromArray(arr))

    // SQL query with an inlined (unregistered) table
    val table = tableEnv.fromDataStream(rowDS)
    tableEnv.registerTable(Key.T_GOODS_RAITING, table)

    val orders = tableEnv.scan(Key.T_GOODS_RAITING)
    val csvPath = s"/Users/sunzhongqian/tmp/csv/${Key.T_GOODS_RAITING}/${System.currentTimeMillis()}/"
    val sink = new CsvTableSink(csvPath, ",", 1, WriteMode.OVERWRITE)

    val result = orders
      .groupBy('goodsId)
      .select('userId.max as 'maxUid)


  }

}
