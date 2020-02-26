import java.lang
import java.util.Date
import java.text.SimpleDateFormat
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object AppMarketingByChannel {

  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dstream=env.addSource(new SimulatedEventSource)
      //event需要是毫秒级别的
      .assignAscendingTimestamps(_.timeStamp)
      .filter(_.behavior!="UNINSTALL")
      //不区分渠道,来一一条记录，就是产生一次行为，所有渠道一起计算
        .map(x=>{
      ("allchanel",1L)
    }).keyBy(_._1)
        .timeWindow(Time.seconds(10L),Time.seconds(1))
        .process(new allChanelProcessWindow)
        .print()
      //区分渠道
      //.map(x=>((x.behavior,x.channel),1))
      //.keyBy(_._1)
      //.timeWindow(Time.hours(1L),Time.seconds(1))
      //.aggregate(new marketAggregate,new marketWindow)
//      .print()

    env.execute()
  }
}

case class allChanel(startWindow:String,endWindow:String,count:Long)
class allChanelProcessWindow extends ProcessWindowFunction[(String,Long),allChanel,String,TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Long)],
                       out: Collector[allChanel]): Unit ={
    val startwindow=formatTs(context.window.getStart)
    val endwindow=formatTs(context.window.getEnd)
    out.collect(allChanel(startwindow,endwindow,elements.size.toLong))
  }

  def formatTs(ts:Long):String={
      val formats=new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
      formats.format(new Date(ts))
  }
}


case class marketResult(behavior:String,channel:String,windowStart:String,windowEnd:String,count:Long)

class marketWindow extends WindowFunction[Long,marketResult,(String,String),TimeWindow] {
  override def apply(key: (String, String), window: TimeWindow, input: Iterable[Long],
                     out: Collector[marketResult]): Unit =
    {

      out.collect(marketResult(key._1,key._2,formatTs(window.getStart),formatTs(window.getEnd),input.iterator.next()))
    }

  private def formatTs(time:Long): String ={
    val df=new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    df.format(new Date(time))
  }

}

//进来的还是包括key
class marketAggregate extends AggregateFunction[((String,String),Int),Long,Long] {
  override def add(in: ((String, String), Int), acc: Long): Long = acc+1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

case class MarketingUserBehavior(userId:String,behavior:String,channel:String,timeStamp:Long)

class SimulatedEventSource extends RichSourceFunction[MarketingUserBehavior] {

  var running=true

  val channelSet=Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")

  val behaviorTypes=Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")

  val rand=Random

  override def cancel(): Unit = running=false

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit ={
    val maxCount=Long.MaxValue
    var count=0

    while (count<maxCount&&running){
      val id=UUID.randomUUID().toString
      val behavior=behaviorTypes(rand.nextInt(behaviorTypes.length))
      val channel =channelSet(rand.nextInt(channelSet.length))
      val ts=System.currentTimeMillis()
      sourceContext.collect(MarketingUserBehavior(id,behavior,channel,ts))
      count+=1
      TimeUnit.MILLISECONDS.sleep(5L)
    }
  }
}
