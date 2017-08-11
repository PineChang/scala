package cn.com.pateo.stress_log.process


import java.text.SimpleDateFormat
import java.util.Date

import com.pateo.bigdata.constant.ConnectionPool
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

import scala.util.parsing.json.JSON

/**
  * Created by Administrator on 2017-07-13.
  */
object StressLogProcess {
  val sdf = new SimpleDateFormat("yyyyMMdd")

  import java.util.Calendar

  val now: Calendar = Calendar.getInstance
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop")

/*
    if(args.length < 4){
      //如果传递进来的参数小于4个说明不正常
      System.err.println(
        s"""
           |Usage: DirectKafkaStressLogProcess <zkQuorum> <group> <topics> <numThreads>
         """.stripMargin
      )
      //退出程序
      System.exit(1)

    }*/
    Logger.getLogger("org").setLevel(Level.WARN)

    //将传递进来的参数变为数组
    val Array(zkQuorum,group,topics,numThread) = Array("172.16.4.66:2181,172.16.4.67:2181,172.16.4.68:2181", "g0", "docker_message", "1")
    val sparkConf =  new SparkConf().setAppName("oneSecondStressTest")
     // sparkConf.setMaster("local[*]")
      sparkConf.set("spark.default.parallelism", "2")
   // sparkConf.set("spark.streaming.kafka.maxRatePerPartiton","5")
    //sparkConf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    //采用1s生成一个DStream
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
    //对数进行过滤，拿到符合条件的也就是压力测试相关的日志行
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val filteredStream = kafkaDStream.map(_._2).filter(line => {
      if(line.isEmpty() || line == null || !line.contains("container_id")|| !line.contains("PerflogEntity")){
        false
      }else{
        true
      }
    }).map(logjson => {
      implicit val formats = DefaultFormats
      val docker_message = parse(logjson).extract[DockerMessage]
      var stresslog = docker_message.log.substring(docker_message.log.indexOf("{"))
      val b = JSON.parseFull(stresslog)
      val returnAny =  b match {
        case Some(map: Map[String, Any]) => map
        case None => println("Parsing failed")
        case other => println("UnkCalendar.getInstance()n data structure: " + other)
      }
      returnAny
    }).filter(any => {
      if(any.isInstanceOf[Map[String,Any]]) true else false
    })
    //解析为map格式，然后得到想要的数据

   val tupledStream = filteredStream.map(someline => {
           //println(rdd.collect.foreach(println))
           val jsonmap = someline.asInstanceOf[Map[String,Any]].get("PerflogEntity").get.asInstanceOf[Map[String,Any]]
           val bizType = jsonmap.getOrElse("bizType","").toString
           val clientid = jsonmap.getOrElse("clientid","").toString
           val threadid = jsonmap.getOrElse("threadid","").toString.toInt
           val stat  = jsonmap.getOrElse("stat","").toString
           val finishtime =  jsonmap.getOrElse("finishtime",0).toString.toLong
           val starttime = jsonmap.getOrElse("starttime",0).toString.toLong

           val costtime = jsonmap.getOrElse("costtime",0).toString.toLong
           val exception = jsonmap.getOrElse("exception",0).toString
           //bizinfo信息
           val  bizinfo = jsonmap.getOrElse("bizinfo","{}").asInstanceOf[Map[String,Any]]
           val bizinfo_msname = bizinfo.getOrElse("msname","").toString
           val bizinfo_classname = bizinfo.getOrElse("classname","").toString
           var bizinfo_methodname = bizinfo.getOrElse("methodname","").toString
           var bizinfo_callservicename = bizinfo.getOrElse("callservicename","").toString
           var bizinfo_callservicemethod=bizinfo.getOrElse("callservicemethod","").toString
          //sysinfo信息
           val sysinfo = jsonmap.getOrElse("sysinfo","()").asInstanceOf[Map[String,Any]]
           val sysinfo_cpu  = sysinfo.getOrElse("cpu","").toString
           //sysinfo_mem信息
           val sysinfo_mem = sysinfo.getOrElse("mem","()").asInstanceOf[Map[String,Any]]
           val sysinfo_mem_heap = sysinfo_mem.getOrElse("heap","()").asInstanceOf[Map[String,Any]]
           val sysinfo_mem_noheap = sysinfo_mem.getOrElse("nonheap","()").asInstanceOf[Map[String,Any]]

           val sysinfo_mem_heap_init = sysinfo_mem_heap.getOrElse("init","").toString
           val sysinfo_mem_heap_used = sysinfo_mem_heap.getOrElse("used","").toString
           val sysinfo_mem_heap_committed = sysinfo_mem_heap.getOrElse("committed","").toString
           val sysinfo_mem_heap_max = sysinfo_mem_heap.getOrElse("max","").toString
          // val sysinfo_mem_heap_multi = sysinfo_mem_heap_init+"-"+sysinfo_mem_heap_used+"-"+sysinfo_mem_heap_committed+"-"+sysinfo_mem_heap_max

           val sysinfo_mem_noheap_init = sysinfo_mem_noheap.getOrElse("init","").toString
           val sysinfo_mem_noheap_used = sysinfo_mem_noheap.getOrElse("used","").toString
           val sysinfo_mem_noheap_committed = sysinfo_mem_noheap.getOrElse("committed","").toString
           val sysinfo_mem_noheap_max = sysinfo_mem_noheap.getOrElse("max","").toString
           //val  sysinfo_mem_noheap_multi=sysinfo_mem_noheap_init+"-"+sysinfo_mem_noheap_used+"-"+sysinfo_mem_noheap_committed+"-"+sysinfo_mem_noheap_max

           (bizType,clientid,threadid,stat,finishtime,starttime,costtime,exception,
             bizinfo_msname,bizinfo_classname,bizinfo_methodname,bizinfo_callservicename,bizinfo_callservicemethod,
             sysinfo_cpu,
             sysinfo_mem_heap_init,sysinfo_mem_heap_used,sysinfo_mem_heap_committed,sysinfo_mem_heap_max,
             sysinfo_mem_noheap_init,sysinfo_mem_noheap_used,sysinfo_mem_noheap_committed,sysinfo_mem_noheap_max)

         }
       )
    //将数据发往mysql方向
    val toMysqlStream = tupledStream.foreachRDD(rdd => {

      //val oneRdd = rdd.repartition(1)
          rdd.foreachPartition(iterator=> {
            val conn = ConnectionPool.getConnection()
            val stmt = conn.createStatement()
            try{
            iterator.foreach(tu => {
              val sql: String = "insert into stress_log values ('" +
                tu._1 + "','" +
                tu._2 + "'," +
                tu._3 + ",'" +
                tu._4 + "'," +
                tu._5 + "," +
                tu._6 + "," +
                tu._7 + ",'" +
                tu._8 + "','" +
                tu._9 + "','" +
                tu._10 + "','" +
                tu._11 + "','" +
                tu._12 + "','" +
                tu._13 + "','" +
                tu._14 + "','" +
                tu._15 + "','" +
                tu._16 + "','" +
                tu._17 + "','" +
                tu._18 + "','" +
                tu._19 + "','" +
                tu._20 + "','" +
                tu._21 + "','" +
                tu._22 + "')"
              val res = stmt.executeUpdate(sql)
            })
            } catch{
                case t:Throwable =>t.printStackTrace()
            }finally {
                ConnectionPool.returnConnection(conn)
            }
          })





     // rdd.collect.foreach(println)

      /*
          .groupByKey().mapValues(iter => {
          val throughput = iter.toList.size/1
          val meanDelay = iter.toList.foldLeft(0)((sum,i)=>(sum + i).toInt)/1
         // println("吞吐量"+throughput)
          (throughput,meanDelay)
        }).map(t => (t._1._1,t._1._2,t._1._3,t._2._1,t._2._2))
         tupleRdd.collect.foreach(println)
        })

 */

    })

    //将数据发往hdfs方向
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
       import hiveContext.implicits._
       hiveContext.sql("use stress_project")
    val toHiveStream  = tupledStream.foreachRDD(rdd =>{
       val rdd1 = rdd.map(tu => StressLog(tu._1,tu._2,tu._3,tu._4,tu._5,tu._6,tu._7,tu._8,tu._9,tu._10,tu._11,tu._12,tu._13,tu._14,tu._15,tu._16,tu._17,tu._18,tu._19,tu._20,tu._21,tu._22))//Calendar.getInstance().get(Calendar.YEAR),Calendar.getInstance().get(Calendar.MONTH),Calendar.getInstance().get(Calendar.DATE),Calendar.getInstance().get(Calendar.HOUR),Calendar.getInstance().get(Calendar.MINUTE),Calendar.getInstance().get(Calendar.SECOND)))
         rdd1.toDF().registerTempTable("table1")
        // rdd1.collect.foreach(println)
         var today = sdf.format(new Date())
         hiveContext.sql("insert into table stress_project.stress_log partition(date='"+today+"') select * from table1")
     })

    ssc.start()
    ssc.awaitTermination()

  }

}


class DockerMessage(val container_id: String,val container_name: String,val source: String, var log: String)

case class StressLog(bizType:String,clientid:String,threadid:Int,stat:String,finishtime:Long,starttime:Long,costtime:Long,exception:String,
                     bizinfo_msname:String,bizinfo_classname:String,bizinfo_methodname:String,bizinfo_callservicename:String,bizinfo_callservicemethod:String,
                     sysinfo_cpu:String,
                     sysinfo_mem_heap_init:String, sysinfo_mem_heap_used:String, sysinfo_mem_heap_committed:String, sysinfo_mem_heap_max:String,
                     sysinfo_mem_noheap_init:String, sysinfo_mem_noheap_used:String, sysinfo_mem_noheap_committed:String, sysinfo_mem_noheap_max:String
                    )
                     //create_year:Int,create_month:Int,create_day:Int,create_hour:Int,create_minute:Int,create_second:Int)

