package pers.chenqian.flink.practices.selector

import java.lang

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import pers.chenqian.flink.practices.constants.Idx

class MyOutputSelector extends OutputSelector[Array[Double]] {

  override def select(value: Array[Double]): Iterable[String] = {
    return Seq(value(Idx.STAY_MS).toString).toList
  }

}
