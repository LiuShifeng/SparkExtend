import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.math._

/**
 * Created by LiuShifeng on 2015/2/2.
 */
object Template{
  def test(sc:SparkContext): Unit ={
    val callFile = sc.textFile(CallMapBuild.JanCDRPath)
    val callMap = CallMapBuild.UnDirectedFrequencyDurationDistributionRelationMap(callFile)
    val workingHoursMap = callMap.map(x=>{
      val workingHoursDistribution = isWorkingHoursArrayBuffer(x._2._5)
      (x._1,(x._2._1,x._2._2,x._2._3,x._2._4,x._2._5,workingHoursDistribution))
    })
    val workingHoursClosenessDistanceMap = closenessCalculate(workingHoursMap)
    workingHoursClosenessDistanceMap.map(x=>x._1._1+","+x._1._2+","+x._2._1._1+","+x._2._1._2+","+x._2._1._3+","+x._2._1._4+","+x._2._2._1+","+x._2._2._2+","+x._2._2._3+","+x._2._2._4+","+x._2._2._5+","+x._2._2._6+","+x._2._2._7(0)+","+x._2._2._7(1)).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanUnDirectedWorkingHoursClosenessDistanceMap")
    val workmatesMap = workingHoursClosenessDistanceMap.filter(x=>x._2._2._7(0)>x._2._2._7(1))
    val relativesMap = workingHoursClosenessDistanceMap.filter(x=>x._2._2._7(0)<x._2._2._7(1))
    val unknowMap = workingHoursClosenessDistanceMap.filter(x=>x._2._2._7(0)==x._2._2._7(1))
    workmatesMap.map(x=>x._1._1+","+x._1._2+","+x._2._1._1+","+x._2._1._2+","+x._2._1._3+","+x._2._1._4+","+x._2._2._1+","+x._2._2._2+","+x._2._2._3+","+x._2._2._4+","+x._2._2._5+","+x._2._2._6+","+x._2._2._7(0)+","+x._2._2._7(1)).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanUnDirectedworkmatesMap")
    relativesMap.map(x=>x._1._1+","+x._1._2+","+x._2._1._1+","+x._2._1._2+","+x._2._1._3+","+x._2._1._4+","+x._2._2._1+","+x._2._2._2+","+x._2._2._3+","+x._2._2._4+","+x._2._2._5+","+x._2._2._6+","+x._2._2._7(0)+","+x._2._2._7(1)).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanUnDirectedrelativesMap")
    unknowMap.map(x=>x._1._1+","+x._1._2+","+x._2._1._1+","+x._2._1._2+","+x._2._1._3+","+x._2._1._4+","+x._2._2._1+","+x._2._2._2+","+x._2._2._3+","+x._2._2._4+","+x._2._2._5+","+x._2._2._6+","+x._2._2._7(0)+","+x._2._2._7(1)).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanUnDirectedunknowMap")
    workmatesMap.count
    relativesMap.count
  }
  def isWorkingHoursSingle(t:Long): Boolean ={
    val cald = new DateUnit(t)
    val day = cald.getWeek()
    val hour = cald.getHour()
    if( hour>8 && hour<19 && day>1 && day<7){
      true
    } else{
      false
    }
  }
  def isWorkingHoursArray(timeArray:Array[Long]):Array[Int]={
    val length = timeArray.length
    val result = new Array[Int](2)
    for(i<-0 until length){
      if(isWorkingHoursSingle(timeArray(i))){
        result(0) = result(0) + 1
      }else
        result(1) = result(1) + 1
    }
    result
  }
  def isWorkingHoursArrayBuffer(timeArrayBuffer:ArrayBuffer[Long]):Array[Int]={
    isWorkingHoursArray(timeArrayBuffer.toArray)
  }
  def closenessCalculate(RelationMap:RDD[((String,String),(Array[Double],Array[Double],Array[Double],Array[Double],ArrayBuffer[Long],Array[Int]))])={
    val NeighbourMessage = RelationMap.map(x=>{
      val user = x._1._1
      val frequencyHourDistribution = x._2._1
      val frequencyWeekDistribution = x._2._2
      val neighbourNum = 1
      val durationHourDistribution = x._2._3
      val durationWeekDistribuion = x._2._4
      val callTime = x._2._5
      val maxF = frequencyHourDistribution.sum
      val sumF = frequencyHourDistribution.sum
      val maxT = durationHourDistribution.sum
      val sumT = durationHourDistribution.sum
      val interval = (callTime.max-callTime.min)/maxF
      (user,(maxF,maxT,neighbourNum,sumF,sumT,interval,interval,frequencyHourDistribution,frequencyWeekDistribution,durationHourDistribution,durationWeekDistribuion))
    }).reduceByKey((x,y)=>{
      val maxF = max(x._1,y._1)
      val maxT = max(x._2,y._2)
      val neighbourNum = x._3+y._3
      val sumF = x._4+y._4
      val sumT = x._5+y._5
      val maxI = max(x._6,y._6)
      val sumI = x._7+y._7
      val frequencyHourDistribution = new Array[Double](24)
      val durationHourDistribution = new Array[Double](24)
      val frequencyWeekDistribution = new Array[Double](7)
      val durationWeekDistribuion = new Array[Double](7)
      for(i<-0 until 24){
        frequencyHourDistribution(i) = x._8(i) + y._8(i)
        durationHourDistribution(i) = x._10(i) + y._10(i)
      }
      for(i<-0 until 7){
        frequencyWeekDistribution(i) = x._9(i) + y._9(i)
        durationWeekDistribuion(i) = x._11(i) + y._11(i)
      }
      (maxF,maxT,neighbourNum,sumF,sumT,maxI,sumI,frequencyHourDistribution,frequencyWeekDistribution,durationHourDistribution,durationWeekDistribuion)
    })
    val CDRCloseness = RelationMap.map(x=>(x._1._1,(x._1._2,x._2._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6))).join(NeighbourMessage).map(x=>{
      val user = x._1
      val neighbour = x._2._1._1
      val frequencyHourDistribution = x._2._1._2
      val frequencyWeekDistribution = x._2._1._3
      val durationHourDistribution = x._2._1._4
      val durationWeekDistribution = x._2._1._5
      val callTime = x._2._1._6.toArray
      val workingHoursDistribution = x._2._1._7
      val maxF = x._2._2._1.toDouble
      val maxT = x._2._2._2
      val neighboursNum = x._2._2._3.toDouble
      val sumF = x._2._2._4.toDouble
      val sumT = x._2._2._5
      val maxI = x._2._2._6
      val sumI = x._2._2._7
      val userFrequencyHourDistribution = x._2._2._8
      val userFrequencyWeekDistribution = x._2._2._9
      val userDurationHourDistribution = x._2._2._10
      val userDurationWeekDistribution = x._2._2._11

      val f = frequencyHourDistribution.sum/maxF
      val t = durationHourDistribution.sum/maxT
      val i =(callTime.max-callTime.min)/(maxI*frequencyHourDistribution.sum)
      val uF = sumF/(neighboursNum*maxF)
      val uT = sumT/(neighboursNum*maxT)
      val uI = sumI/(neighboursNum*maxI)
      //val T = FFT.maxF(callTime,86400000)
      val closenessDistance = Closeness.closenessDistance(f,t)
      val closenessDistanceR = if(Closeness.closenessDistance(uF,uT) == 0)  1.0 else closenessDistance/Closeness.closenessDistance(uF,uT)
      //val closenessDistanceInterval = Closeness.closenessDistanceInterval(f,t,i)
      //val closenessDistanceIntervalR = closenessDistanceInterval/Closeness.closenessDistanceInterval(uF,uT,uI)
      //val closenessInterval = Closeness.closenessInterval(f,i)
      //val closenessIntervalR = closenessInterval/Closeness.closenessInterval(uF,uI)
      //val closenessPeriodicityTimeDistribution = Closeness.closenessPeriodicityTimeDistribution(0.5,callTime,frequencyHourDistribution,userFrequencyHourDistribution)
      val closenessIDF = Closeness.closenessIDF(frequencyHourDistribution,userFrequencyHourDistribution,durationHourDistribution,userDurationHourDistribution)
      //val closenessDistanceT = if(T>0){closenessDistance/T}else{closenessDistance/0.01}
      //val closenessIDFT = closenessIDF*T
      val closenessIDFDistance = Closeness.closenessIDFDistance(frequencyHourDistribution,userFrequencyHourDistribution,durationHourDistribution,userDurationHourDistribution,f,t)

      ((user,neighbour),((closenessDistance,closenessDistanceR,closenessIDF,closenessIDFDistance),(f,t,i,uF,uT,uI,workingHoursDistribution)))
    })
    CDRCloseness
  }
}
