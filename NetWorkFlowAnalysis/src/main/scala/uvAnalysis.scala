


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

case class uvClass(userId:Long,behavior:String,timestamp:Long)
object uvAnalysis {

  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dstream=env.readTextFile("D:\\gitPro\\userBehaviorAnalysis\\NetWorkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(x=>{
      val arr=x.split(",")
      uvClass(arr(0).trim.toLong,arr(3).trim,arr(4).trim.toLong)
    })
      .assignAscendingTimestamps(_.timestamp*1000)
      .filter(_.behavior=="pv")
      .timeWindowAll(Time.seconds(60*60))
      .apply(new myWindow1)
      .print()

    env.execute()
  }

}
//[in,out,window]
class myWindow1 extends AllWindowFunction[uvClass,(Long,Long),TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[uvClass],
                     out: Collector[(Long, Long)]): Unit ={
   var set=Set[Long]()
    for (i<-input){
      set+=i.userId
    }

    out.collect((window.getEnd,set.size.toLong))
  }

}