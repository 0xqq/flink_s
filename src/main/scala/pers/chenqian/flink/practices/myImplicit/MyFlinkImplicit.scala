package pers.chenqian.flink.practices.myImplicit

import org.apache.flink.table.expressions.{Expression, ExpressionParser}

import scala.collection.mutable.ListBuffer

object MyFlinkImplicit {


  implicit def strSeqToExpreSeq(strSeq: Seq[String]): Seq[Expression] = {
    val lb = new ListBuffer[Expression]
    for (str <- strSeq) {
      lb += ExpressionParser.parseExpression(str)
    }

    return lb.toSeq
  }



}
