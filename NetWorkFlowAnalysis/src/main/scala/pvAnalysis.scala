import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

case class pvClass(behavior:String,timestamp:Long)
object pvAnalysis {
  def main(args: Array[String]): Unit = {
      val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream=env.readTextFile("D:\\gitPro\\userBehaviorAnalysis\\NetWorkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv").map(x=>{
      val arr=x.split(",")
      pvClass(arr(3).trim,arr(4).trim.toLong)
    })
      .assignAscendingTimestamps(_.timestamp*1000)
      .filter(_.behavior=="pv")
      //普通计数转成(_，1)形式，再对sum(1)
      .map(x=>("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(60*60))
      .sum(1)
      .print()

    env.execute()
  }
}
