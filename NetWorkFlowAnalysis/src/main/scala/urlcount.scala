import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)
case class UrlViewCount(url:String,windowEnd:Long,count:Long)

object urlcount {

  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dstream=env.readTextFile("D:\\gitPro\\userBehaviorAnalysis\\NetWorkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(text=>{
        val arr= text.split(" ")
        val dftime=new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp=dftime.parse(arr(3)).getTime
        ApacheLogEvent(arr(0).trim,arr(2).trim,timestamp,arr(5).trim,arr(6).trim)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })
      .filter( data => {
        val pattern = "^((?!\\.(css|js)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      } )
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.minutes(5))
      .aggregate(new myAggCount,new myWindow)
      .keyBy(_.windowEnd)
      .process(new TopNFunction(4))
      .print

    env.execute()

  }

}

class TopNFunction(topNum:Int) extends KeyedProcessFunction[Long,UrlViewCount,String] {

  private var itemCounts:ListState[UrlViewCount]=_


  override def open(parameters: Configuration): Unit = {
    itemCounts=getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]
    ("itemCounts",classOf[UrlViewCount]))
  }

  override def processElement(i: UrlViewCount,
                              context: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              collector: Collector[String]): Unit ={
    //保存数据到状态中
      itemCounts.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit =
  {
    //启动定时器说明windowEnd已经结束，该Key window中数据已经都存到state中，把数据从state中取出放到ListBuffer中然后做排序
    var itemList:ListBuffer[UrlViewCount]=new ListBuffer[UrlViewCount]
    import scala.collection.JavaConversions._
    for (i <- itemCounts.get){
      itemList+=i
    }
    //清空状态
    itemCounts.clear()

    //对listBuffer中排序
    val sortedStream=itemList.sortBy(_.count)(Ordering.Long.reverse).take(topNum)

    //输出到一个string中，通过stringbuilder连接
    val sb:StringBuilder=new StringBuilder
    sb.append("-------------------\n")
    sb.append("时间:  ").append(new Timestamp(timestamp-1)).append("\n")

    //使用排序完成的stream,i是下标
    for (i<-sortedStream.indices){
      val item=sortedStream(i)
      sb.append("No: ").append(i+1).append("  Url: ").append(item.url)
        .append("  点击次数").append(item.count).append("\n")
    }

    sb.append("-----------------------------\n")

    Thread.sleep(1000)
    out.collect(sb.toString())
  }
}


class myWindow extends  WindowFunction[Long,UrlViewCount,String,TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit ={
    val ucount=input.iterator.next()
    out.collect(UrlViewCount(key,window.getEnd,ucount))
  }
}

class myAggCount extends AggregateFunction[ApacheLogEvent,Long,Long] {
  override def add(in: ApacheLogEvent, acc: Long): Long = acc+1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long =acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}
