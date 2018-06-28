package pers.chenqian.flink.practices.entities

case class GrVo(goodsId: Double, userId: Double, raiting: Double, viewCounts: Double,
                stayMs: Double, isStar: Double, buyCounts: Double, timestamp: Double) {

}

object GrVo {

  def fromArray(arr: Array[Double]): GrVo = {
    return new GrVo(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7))
  }

}