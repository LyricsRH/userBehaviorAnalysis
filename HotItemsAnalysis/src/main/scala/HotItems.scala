import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.collection.mutable.ListBuffer
import scala.xml.Properties

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String,
                        timestamp: Long)
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {

  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val props=new Properties()
    props.setProperty("bootstrap.servers","localhost:9092")
    props.setProperty("group.id","consumer-group")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset","latest")

     val dstream=env.readTextFile("D:\\gitPro\\userBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
  //  val dstream=env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),props))
      .map(text=>{
      val arr=text.split(",")
      UserBehavior(arr(0).trim.toLong,arr(1).trim.toLong,arr(2).trim.toInt,arr(3).trim,arr(4).trim.toLong)
    })
      //指定时间戳
        .assignAscendingTimestamps(_.timestamp*1000)
      .filter(_.behavior=="pv")
      .keyBy(_.userId)
      .timeWindow(Time.minutes(60),Time.minutes(5))
      .aggregate(new myAggrateFunction,new myWindow)
      .keyBy(_.windowEnd)
      .process(new MyKeyProcess(3))
      .print()

    env.execute()


  }
}

//AggregateFunction <IN, ACC, OUT>
class myAggrateFunction extends AggregateFunction[UserBehavior,Long,Long] {
  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

//自定义聚集函数求平均数
class avgAgg extends AggregateFunction[UserBehavior,(Long,Int),Double] {
  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1+in.timestamp,acc._2+1)

  override def createAccumulator(): (Long, Int) = (0L,0)

  override def getResult(acc: (Long, Int)): Double = acc._1/acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) =
    (acc._1+acc1._1,acc._2+acc1._2)
}


//WindowFunction[IN, OUT, KEY, Window ]
class myWindow extends WindowFunction[Long,ItemViewCount,Long,TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val result = input.iterator.next()
      out.collect(ItemViewCount(key, window.getEnd, result))
  }
}

//KeyedProcessFunction<K, I, O> 这里以windowEnd为key,输入每个ItemViewCount，输出显示结果string串
class MyKeyProcess(topNum:Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {

  var result:ListState[ItemViewCount]=_

  override def open(parameters: Configuration): Unit =
    result=getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item_list",classOf[ItemViewCount]) )

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit =
    {
      result.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd+1)
    }

  // 新到达的数据的EventTime到这个timestamp时候调用onTimer
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit =
  {
    val allitems:ListBuffer[ItemViewCount]=ListBuffer()

    import scala.collection.JavaConversions._
    for (item <- result.get){
      allitems+=item
    }

    //result状态中数据都装到了listbuffer中，状态可以清空等待下次输入
    result.clear()

    val sortedItems= allitems.sortBy(_.count)(Ordering.Long.reverse).take(topNum)

    val resultPrint:StringBuilder=new StringBuilder
    resultPrint.append("==================\n")
    resultPrint.append("时间：").append(new Timestamp(timestamp-1)).append("\n")

    for (ite <- sortedItems.indices){
      val curried:ItemViewCount =sortedItems(ite)
      resultPrint.append("No").append(ite+1).append(": ")
          .append("  商品ID=").append(curried.itemId)
        .append("  浏览次数=").append(curried.count).append("\n")
    }

    resultPrint.append("=====================\n")
    Thread.sleep(1000)
    out.collect(resultPrint.toString())
  }





}