package cn.com.pateo.stress_log.process

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017-07-24.
  */
object kafkaTest {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org").setLevel(Level.WARN)
    val Array(zkQuorum,group,topics,numThread) = Array("172.16.4.66:2181,172.16.4.67:2181,172.16.4.68:2181", "g0", "docker_message", "1")
    val sparkConf =  new SparkConf().setAppName("oneSecondStressTest")
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.default.parallelism", "2")
    import org.apache.spark.SparkContext
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(1))
    //1以下拿到集群的地址，集群的groupId
    import scala.collection.immutable._
    val kafka_params = Map[String,String](
      "zookeeper.connect"  -> zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )
    val topicMap = topics.split(",").map((_,numThread.toInt)).toMap

    val kafkaDStream = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
      ssc,kafka_params,topicMap,StorageLevel.MEMORY_AND_DISK_SER
    )
    kafkaDStream.foreachRDD(rdd =>{
      rdd.collect.foreach(println)
      println(rdd.count())
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
