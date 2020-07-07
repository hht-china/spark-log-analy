package com.hht.session

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
  * 自定义累加器 AccumulatorV2[输入类型,输出类型]
  */
class StepAndVisitLengthAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
  // 定义一个map保存所有聚合数据，也就是定义一个返回值
  private val hashMap: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()

  //是否是初始化状态，对于map就是空
  override def isZero: Boolean = {
    hashMap.isEmpty
  }

  //复制，返回的就是这个累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAccumulator = new StepAndVisitLengthAccumulator
    //synchronized就好像java的锁
    hashMap.synchronized{
      newAccumulator.hashMap ++= this.hashMap
    }
    newAccumulator
  }

  override def reset(): Unit = {
    hashMap.clear()
  }

  override def add(v: String): Unit = {
    //没有这个k的话就放一个（k,0）
    if (!hashMap.contains(v)) {
      hashMap += (v -> 0)
    }
    //更新(k,v+1)
    hashMap.update(v, hashMap(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc: StepAndVisitLengthAccumulator => {
        (this.hashMap /: acc.value) { case (map, (k, v)) => map += (k -> (v + map.getOrElse(k, 0))) }
      }
    }
  }

  //取值
  override def value: mutable.HashMap[String, Int] = {
    this.hashMap
  }
}
