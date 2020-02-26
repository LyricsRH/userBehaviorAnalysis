import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer {

  def main(args: Array[String]): Unit = {
    val props=new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer=new KafkaProducer[String,String](props)

    //从文件中读数据
    val bufferSource=io.Source.fromFile("D:\\gitPro\\userBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line<- bufferSource.getLines()){
      val record:ProducerRecord[String,String]=new ProducerRecord[String,String]("hotitems",line)
         producer.send(record)
    }
    producer.close()

  }
}
