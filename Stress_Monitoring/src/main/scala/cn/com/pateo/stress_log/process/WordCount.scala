package cn.com.pateo.stress_log.process

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-07-21.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)
     sc.parallelize(List("zhang pan","zhang wang","zhang wang","li si","wu zhi","hello jim"), 1).flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_+_).sortBy(_._2, false).collect.foreach(println)
    sc.stop()
  }

}
