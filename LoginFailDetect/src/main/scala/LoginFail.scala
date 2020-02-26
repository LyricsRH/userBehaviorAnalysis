import java.util

import org.apache.flink.api.common.operators.Keys.SelectorFunctionKeys
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

object LoginFail {

  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream=env.readTextFile("D:\\gitPro\\userBehaviorAnalysis\\LoginFailDetect\\src\\main\\resources\\LoginLog.csv")
      .map(x=>{
      val arr=x.split(",")
      LoginEvent(arr(0).trim.toLong,arr(1).trim,arr(2).trim,arr(3).trim.toLong)
    })
        .assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
          override def extractTimestamp(t: LoginEvent): Long = t.eventTime*1000
        })

    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail").within(Time.seconds(2))

    //只是模式匹配，类型并没有变
    val patternStream= CEP.pattern(stream.keyBy(_.userId),loginFailPattern)

    patternStream.select(new PatternFunc)
      .print()
      /*
      .keyBy(_.userId)
      .process(new MatchFunction)
      .print()
*/
    env.execute()
  }
}

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
case class Warning(userID:Long,fistFailTime:Long,LastFailTime:Long,msg:String)

class PatternFunc extends PatternSelectFunction[LoginEvent,Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail= map.get("begin").iterator().next()
    val lastFail= map.get("next").iterator().next()
    Warning(firstFail.userId,firstFail.eventTime,lastFail.eventTime,"fail!!")
  }
}

class MatchFunction extends KeyedProcessFunction[Long,LoginEvent,LoginEvent] {

  lazy val countList=getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]
  ("counts",classOf[LoginEvent]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context,
                              collector: Collector[LoginEvent]): Unit ={
    val lists=countList.get()
    if (i.eventType=="fail"){
      //第一个fail
        if (!lists.iterator().hasNext){
          //每次来一个元素都注册一个定时器，但是只有来的是fail才add,
          context.timerService().registerEventTimeTimer(i.eventTime*1000+2000)
        }
      countList.add(i)
    }else{
      //必须是连续两次失败而且在2s之内
      countList.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext,
                       out: Collector[LoginEvent]): Unit = {
    var counts:ListBuffer[LoginEvent]=new ListBuffer
    import scala.collection.JavaConversions._
    //没有直接获得ListState里面元素数目的方法，也还是要一个一个的取出来
    //这里的get需要导入上面的包，get是一个java方法
    for (i<-countList.get()){
       counts+=i
    }
    countList.clear()

    if (counts.size>1){
      //把存入状态的失败日志输出
      out.collect(counts.head)
    }
  }

}


