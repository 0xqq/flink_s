package pers.chenqian.flink.practices.tsAndWm

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class MyTimestampsAndWatermarks extends AssignerWithPeriodicWatermarks[Array[Double]] {

  val maxOutOfOrderness = 3500L // 3.5 seconds

  var currentMaxTimestamp = 0L

  override def getCurrentWatermark: Watermark = {
    // return the watermark as current highest timestamp minus the out-of-orderness bound
    return new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }

  override def extractTimestamp(element: Array[Double], previousElementTimestamp: Long): Long = {
    val timestamp = element.last.toLong
    currentMaxTimestamp = math.max(timestamp, currentMaxTimestamp)
    return timestamp
  }

}
