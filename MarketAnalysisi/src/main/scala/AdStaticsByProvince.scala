import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object AdStaticsByProvince {

  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream=env.readTextFile("D:\\gitPro\\userBehaviorAnalysis\\MarketAnalysisi\\src\\main\\resources\\AdClickLog.csv")
      .map(x=>{
      val arr=x.split(",")
      AdClickLog(arr(0).trim.toLong,arr(1).trim.toLong,arr(2).trim,arr(3).trim,arr(4).trim.toLong)
    })
      .assignAscendingTimestamps(_.timestamp*1000)
      .keyBy(_.province)
      .timeWindow(Time.seconds(60*60),Time.seconds(5))
      //增量聚集的方法
      .aggregate(new adAggregate,new adWindowFunc)
      //全窗口函数的方法
      //.process(new adProcessWindow)
      .print()
    env.execute()
  }
}

class adWindowFunc extends WindowFunction[Long,CountByProvince,String,TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long],
                     out: Collector[CountByProvince]): Unit =
    {
      out.collect(CountByProvince(window.getEnd,key,input.iterator.next()))
    }
}


class adAggregate extends AggregateFunction[AdClickLog,Long,Long] {
  override def add(in: AdClickLog, acc: Long): Long = acc+1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

class adProcessWindow extends ProcessWindowFunction[AdClickLog,CountByProvince,String,TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[AdClickLog],
                       out: Collector[CountByProvince]): Unit ={
    out.collect(CountByProvince(context.window.getEnd,key,elements.iterator.size))
  }
}

case class AdClickLog(userID:Long,adID:Long,province:String,city:String,timestamp:Long)
case class CountByProvince(windowEnd:Long,province:String,count:Long)

