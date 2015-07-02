import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Created by LiuShifeng on 2015/5/6.
 */
class CDRProfile(val file:RDD[String]) extends Serializable {
  val callerIDPosition = 0
  val callLacPosition = 1
  val callTimePosition = 2
  val callDurationPosition = 3
  val callDirection = 4
  val calleeIDPosition = 5

  def getTimeDistribution() ={
    val userCommunicationInfo = file.map(x=>{
      val s = x.split(",")
      val time = s(callTimePosition)
      val duration = s(callDurationPosition).toDouble

      val result = new Array[((String,String),(Array[Int],Array[Double],Array[Int],Array[Double]))](2)
      val date = new DateUnit(time)
      val hour = date.getHour()
      val day = date.getDay()
      val weekDay = date.getWeek()
      val weekFrequencyArray = new Array[Int](7*24)
      val dayFrequencyArray = new Array[Int](31*24)
      val weekDurationArray = new Array[Double](7*24)
      val dayDurationArray = new Array[Double](31*24)
      weekFrequencyArray((weekDay-1)*(hour-1)) = 1
      weekDurationArray((weekDay-1)*(hour-1)) = duration
      dayFrequencyArray((day-1)*(hour-1)) = 1
      dayDurationArray((day-1)*(hour-1)) = duration

      result(0) = ((s(callerIDPosition),s(calleeIDPosition)),(weekFrequencyArray,weekDurationArray,dayFrequencyArray,dayDurationArray))
      result(1) = ((s(calleeIDPosition),s(calleeIDPosition)),(weekFrequencyArray,weekDurationArray,dayFrequencyArray,dayDurationArray))
      result
    }).flatMap(x=>x).reduceByKey((x,y)=>{
      val weekFrequencyArray = new Array[Int](7*24)
      val dayFrequencyArray = new Array[Int](31*24)
      val weekDurationArray = new Array[Double](7*24)
      val dayDurationArray = new Array[Double](31*24)
      for(i<- 0 until 7*24){
        weekFrequencyArray(i) = x._1(i)+y._1(i)
        weekDurationArray(i) = x._2(i)+y._2(i)
      }
      for(i<-0 until 31*24){
        dayFrequencyArray(i) = x._3(i)+y._3(i)
        dayDurationArray(i) = x._4(i)+y._4(i)
      }
      (weekFrequencyArray,weekDurationArray,dayFrequencyArray,dayDurationArray)
    })
    userCommunicationInfo
  }

  def getNodeFeature()={
    val nodeFeature = file.map(x=> {
      val s = x.split(",")
      val caller = s(callerIDPosition)
      val callee = s(calleeIDPosition)
      val time = s(callTimePosition)
      val duration = s(callDurationPosition).toDouble
      val direct = s(callDirection)
      if (direct.equals("0"))
        ((caller, callee), (1, duration))
      else
        ((callee, caller), (1, duration))
    }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>{
      val caller = x._1._1
      val callee = x._1._2
      val frequency = x._2._1
      val duration = x._2._2
      var callerNeighbourSet = Set(callee)
      var calleeNeighbourSet = Set(caller)
      val result = new Array[(String,(Set[String],Int,Int,Double,Int,Int,Double))](2)
      result(0) = (caller,(callerNeighbourSet,1,frequency,duration,0,0,0))
      result(1) = (callee,(calleeNeighbourSet,0,0,0,1,frequency,duration))
      result
    }).flatMap(x=>x).reduceByKey((x,y)=>{
      val neighbourSet = x._1.union(y._1)
      (neighbourSet,x._2+y._2,x._3+y._3,x._4+y._4,x._5+y._5,x._6+y._6,x._7+y._7)
    })
    nodeFeature
  }
}
