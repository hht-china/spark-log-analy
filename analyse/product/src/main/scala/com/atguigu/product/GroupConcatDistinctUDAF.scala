package com.atguigu.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * @author hongtao.hao
  * @date 2020/7/7
  */
class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
  //输入数据类型
  //Nil是空List
  // :: 用于的是向队列的头部追加数据
  //等价于new StructType() .add("age", IntegerType).add("name", StringType)
  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType) :: Nil)

  //聚合缓冲区中值的数据类型
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo", StringType) :: Nil)

  //输出数据类型
  override def dataType: DataType = StringType

  //如果这个函数是确定的，即给定相同的输入，总是返回相同的输出
  override def deterministic: Boolean = true

  //  初始化给定的聚合缓冲区，即聚合缓冲区的零值。
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //我们只进行聚合，也就是缓冲区的第一个值
    buffer(0) = ""
    //如果还需要别的操作，比如统计count
    //buffer(1) = 0
  }

  //  使用来自输入的新输入数据更新给定的聚合缓冲区。
  // 每个输入行调用一次。（同一分区）
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var value: String = buffer.getString(0)
    val inputValue: String = input.getString(0)

    //做一个去重，我们这个sql已经拿到area，pid，count
    // 重复的城市信息对我们没用
    if (!value.contains(inputValue)) {
      if ("".equals(value)) {
        value += inputValue
      } else {
        value += "," + inputValue
      }
    }

    //更新下缓冲区第0个值的数据
    buffer.update(0, value)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var value1: String = buffer1.getString(0)
    val value2: String = buffer2.getString(0)

    //合并的时候也要去重--去掉两个要合并的数据的重复的部分
    for (v <- value2.split(",")) {
      if (!value1.contains(v)) {
        if ("".equals(value1)) {
          value1 += v
        } else {
          value1 += "," + v
        }
      }
    }

    buffer1.update(0, value1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
