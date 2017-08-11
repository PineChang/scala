import scala.util.parsing.json.JSON

/**
  * Created by Administrator on 2017-07-13.
  */

object Test {

  def main(args: Array[String]): Unit = {
    val s:String = "zhong"
    val b:String = "guo"
   val  a:String = "skkkkkk('"+
                    s + "')"

    println(a)

    /*
   // var  stresslog = "{"
    var stresslog="07-11 13:56:02 914[INFO]com.pateo.qingcloud.framework.common.perflog.PateoPerfLogger.info(PateoPerfLogger.java:45){\"PerflogEntity\":{\"bizType\": \"BIZ-SERVICE\",\"clientid\": \"null\",\"threadid\": \"179\",\"sysinfo\": {\"cpu\": \"null\" ,\"mem\": {\"heap\": {\"init\":\"1052770304\", \"used\":\"489160696\", \"committed\":\"1052770304\", \"max\":\"1073741824\"} , \"nonheap\": {\"init\":\"2555904\", \"used\":\"110851160\", \"committed\":\"114057216\", \"max\":\"419430400\"}}},\"bizinfo\": {\"msname\": \"msuser\", \"classname\": \"com.pateo.qingcloud.base.user.controller.UserFavoriteController\", \"methodname\": \"add\", \"callservicename\": \"null\", \"callservicemethod\": \"null\" },\"stat\": \"success\",\"starttime\": \"1499752562593\",\"finishtime\": \"1499752562914\",\"costtime\": \"321\",\"exception\": \"null\"}}"
    var stresslog1=stresslog.indexOf("{")
    var  stresslog2 = stresslog.substring(stresslog.indexOf("{"))
    val b = JSON.parseFull(stresslog2)
    val returnVal =  b match {
      case Some(map: Map[String, Any]) => map
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }
    println( returnVal.asInstanceOf[Map[String,Any]].get("PerflogEntity").get.asInstanceOf[Map[String,Any]].get("starttime"))
  }
*/
  }
}
