import javafx.scene.effect.Bloom

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
import org.apache.flink.streaming.api.scala._

case class uvCount(windowEnd:Long,count:Long)

object UniqueVisitor {

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
      .map(x=>("dumpyKey",x.userId))
        .keyBy(_._1)
      .timeWindow(Time.seconds(60*60))
      //触发器，每次来一个就存到redis里并且把window中的内存state删掉
      .trigger(new myTrigger)
      //每个元素都执行window function
      .process(new uvCountWithBloom)
      .print()

    env.execute()
  }
}

//布隆过滤器其实就定义了一个hash函数
class Bloom(size:Long) extends Serializable{
  private val cap = size
  def hash(value: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    (cap - 1) & result
  }

}

class uvCountWithBloom extends ProcessWindowFunction[(String,Long),uvCount,String,TimeWindow] {

  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)],
                       out: Collector[uvCount]): Unit = {
    val storeKey = context.window.getEnd.toString
    var count = 0L
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }

    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    val isExist = jedis.getbit(storeKey, offset)
    if (!isExist) {
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(uvCount(storeKey.toLong, count + 1))
    } else {
      out.collect(uvCount(storeKey.toLong, count))
    }
  }

}

class myTrigger extends Trigger[(String,Long),TimeWindow] {
  //eventTime<=WaterMark则执行窗口事件 fire,这里由于都存在redis中，不用执行不用fire
  override def onEventTime(l: Long, w: TimeWindow,
                           triggerContext: Trigger.TriggerContext): TriggerResult ={
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult =
    TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit ={}

  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      //每来一个元素，就触发窗口事件并且清空、
    TriggerResult.FIRE_AND_PURGE
  }
}
