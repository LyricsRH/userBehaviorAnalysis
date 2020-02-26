


import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeOut {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream = env.readTextFile("D:\\gitPro\\userBehaviorAnalysis\\OrderTimeOut\\src\\main\\resources")
      .map( data => {
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
    })
      .assignAscendingTimestamps(_.eventTime * 1000)

    val orderPattern=Pattern.begin[OrderEvent]("begin").where(_.eventType=="create")
      .followedBy("follow").where(_.eventType=="pay").within(Time.minutes(15))

    val timeOutTag=new OutputTag[OrderResult]("orderTimeOut")

    val patternStream =CEP.pattern(orderEventStream.keyBy(_.orderId),orderPattern)

     val resultStream=patternStream.select(timeOutTag,new TimeOutSelect,new OrderPaySelect)

    resultStream.print("pay")
    resultStream.getSideOutput(timeOutTag).print("timeOut")

    env.execute()

  }
}

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)
case class OrderResult(orderId: Long, eventType: String)

class TimeOutSelect extends PatternTimeoutFunction[OrderEvent,OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    //因为超时所以只有begin的元素
    val timeOut=map.get("begin").iterator().next()
    OrderResult(timeOut.orderId,timeOut.eventType)
  }
}

class OrderPaySelect extends PatternSelectFunction[OrderEvent,OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val first=map.get("begin").iterator().next()
    val next=map.get("follow").iterator().next()
    OrderResult(first.orderId,next.eventType)
  }
}