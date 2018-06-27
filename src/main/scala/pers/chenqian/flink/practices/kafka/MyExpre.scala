package pers.chenqian.flink.practices.kafka

import org.apache.flink.table.expressions.Expression

class MyExpre extends Expression {
  override def productElement(n: Int): Any = ???

  override def productArity: Int = ???

  override def canEqual(that: Any): Boolean = ???
}
