import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object AdStaticsWithBlack {
  val blackOutputTag=  new OutputTag[BlackListWarning]("blackList")
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
      .keyBy(x=>(x.userID,x.adID))
      .process(new BlackFunction(100))

      val adCountStream=stream
      .keyBy(_.province)
      .timeWindow(Time.seconds(60*60),Time.seconds(5))
      //增量聚集的方法
      .aggregate(new adAggregate,new adWindowFunc)
      //全窗口函数的方法
      //.process(new adProcessWindow)
      .print()

     stream.getSideOutput(blackOutputTag).print("黑名单")
    env.execute()
  }

  class BlackFunction(maxSize:Long) extends KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog] {

    lazy val countState:ValueState[Long]=getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState",classOf[Long]))
    lazy val isFirstSend:ValueState[Boolean]=getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("isFirstSend",classOf[Boolean])
    )
    lazy val resetTime:ValueState[Long]=getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTime",classOf[Long]))



    override def processElement(i: AdClickLog, context: KeyedProcessFunction[(Long, Long),
      AdClickLog, AdClickLog]#Context, collector: Collector[AdClickLog]): Unit ={
      val addCount=countState.value()
      if (addCount==0){
        val ts=(context.timerService().currentProcessingTime()/(24*60*60*1000)+1)*(24*60*60*1000)
        resetTime.update(ts)
        context.timerService().registerProcessingTimeTimer(ts)
      }
      if (addCount>maxSize){
        //判断是否发送过黑名单信息，
        if (!isFirstSend.value()){
          //说明已经发送过
          isFirstSend.update(true)
          //output侧输出流，指定outputTag，用于获取，后面是侧输出流输出信息
          context.output(blackOutputTag,BlackListWarning(i.userID,i.adID,"click over"+maxSize+"today"))
        }
        return
      }
      //更新次数
      countState.update(addCount+1)
      //这些满足条件的没被过滤正常输出
      collector.collect(i)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]) = {
      //达到凌晨清零的时间，计数器和是否发送过都清零
      if(timestamp==resetTime.value()){
        isFirstSend.clear()
        countState.clear()
      }
    }
  }

}

case class BlackListWarning(userID:Long,adID:Long,msg:String)


