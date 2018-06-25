package pers.chenqian.flink.practices.basic

import org.apache.flink.api.common.accumulators.DoubleMaximum
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import pers.chenqian.flink.practices.constants.Idx

trait WithKafkaFailed {

  /**
    * 跑起后没输出
    */
  def windowF2(mappedDS: DataStream[Array[Double]]) = {
    import org.apache.flink.api.scala._

    mappedDS
      .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[Array[Double]] {
        override def extractAscendingTimestamp(element: Array[Double]): Long = {
          //          val doubleVal = System.currentTimeMillis() + element(Idx.USER_ID)
          //          return doubleVal.toLong
          return System.currentTimeMillis()
        }
      })
      .keyBy(_(Idx.GOODS_ID))
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
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


}
