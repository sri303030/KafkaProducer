import kafka.serializer.StringDecoder
import java.util.HashMap

//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaCons {
  def main(args: Array[String]) {
 

    StreamingExample.setStreamingLogLevels()

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(300))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
  
 val words=lines.map ( x => x.split(" ").toList)
   .filter{ x=> x.length > 1}
  .reduceByKeyAndWindow(_ + _,_-_, Minutes(10), Seconds(2), 1)
  .filter(y=> y._2 > 3)
 .print()//.saveAsTextFiles("ip.counts.txt")
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
