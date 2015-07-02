import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.math._

object CallMapBuild extends Serializable{
  //neighbours build
  val CallerPosition = 0
  val AreaPosition = 1
  val TimePosition = 2
  val DurationPosition = 3
  val CalleePosition = 5
  val ControlPosition = 4
  //file path
  val JanCDRPath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201401.csv"
  val FebCDRPath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201402.csv"
  val MarCDRPath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201403.csv"
  val AprCDRPath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201404.csv"
  val MayCDRPath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201405.csv"
  val JunCDRPath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201406.csv"
  val JulCDRPath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201407.csv"
  val AugCDRPath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201408.csv"
  val SepCDRPath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201409.csv"
  val TotalCDRPath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_20140?.csv"
  /**
  /**
   * CDR file and CDR map Build
   */
  val JanCDRFile = sc.textFile(JanCDRPath)
  val FebCDRFile = sc.textFile(FebCDRPath)
  val MarCDRFile = sc.textFile(MarCDRPath)
  val AprCDRFile = sc.textFile(AprCDRPath)
  val MayCDRFile = sc.textFile(MayCDRPath)
  val JunCDRFile = sc.textFile(JunCDRPath)
  val JulCDRFile = sc.textFile(JulCDRPath)
  val AugCDRFile = sc.textFile(AugCDRPath)
  val SepCDRFile = sc.textFile(SepCDRPath)
  val TotalCDRFile =sc.textFile(TotalCDRPath)

  /**
   * NeighbourMap
   */
  val JanCallingNeighbour = DirectedRelationMap(JanCDRFile)
  val JanCalledNeighbour = ReversedDirectedRelationMap(JanCDRFile)
  val JanReCallNeighbour = UndirectedRelationMap(JanCDRFile)
  val JanBidirectionNeighbour = BidirectedRelationMap(JanCDRFile)

  val FebCallingNeighbour = DirectedRelationMap(FebCDRFile)
  val FebCalledNeighbour = ReversedDirectedRelationMap(FebCDRFile)
  val FebReCallNeighbour = UndirectedRelationMap(FebCDRFile)
  val FebBidirectionNeighbour = BidirectedRelationMap(FebCDRFile)

  val MarCallingNeighbour = DirectedRelationMap(MarCDRFile)
  val MarCalledNeighbour = ReversedDirectedRelationMap(MarCDRFile)
  val MarReCallNeighbour = UndirectedRelationMap(MarCDRFile)
  val MarBidirectionNeighbour = BidirectedRelationMap(MarCDRFile)

  val AprCallingNeighbour = DirectedRelationMap(AprCDRFile)
  val AprCalledNeighbour = ReversedDirectedRelationMap(AprCDRFile)
  val AprReCallNeighbour = UndirectedRelationMap(AprCDRFile)
  val AprBidirectionNeighbour = BidirectedRelationMap(AprCDRFile)

  val MayCallingNeighbour = DirectedRelationMap(MayCDRFile)
  val MayCalledNeighbour = ReversedDirectedRelationMap(MayCDRFile)
  val MayReCallNeighbour = UndirectedRelationMap(MayCDRFile)
  val MayBidirectionNeighbour = BidirectedRelationMap(MayCDRFile)

  val JunCallingNeighbour = DirectedRelationMap(JunCDRFile)
  val JunCalledNeighbour = ReversedDirectedRelationMap(JunCDRFile)
  val JunReCallNeighbour = UndirectedRelationMap(JunCDRFile)
  val JunBidirectionNeighbour = BidirectedRelationMap(JunCDRFile)

  val JulCallingNeighbour = DirectedRelationMap(JulCDRFile)
  val JulCalledNeighbour = ReversedDirectedRelationMap(JulCDRFile)
  val JulReCallNeighbour = UndirectedRelationMap(JulCDRFile)
  val JulBidirectionNeighbour = BidirectedRelationMap(JulCDRFile)

  val AugCallingNeighbour = DirectedRelationMap(AugCDRFile)
  val AugCalledNeighbour = ReversedDirectedRelationMap(AugCDRFile)
  val AugReCallNeighbour = UndirectedRelationMap(AugCDRFile)
  val AugBidirectionNeighbour = BidirectedRelationMap(AugCDRFile)

  val SepCallingNeighbour = DirectedRelationMap(SepCDRFile)
  val SepCalledNeighbour = ReversedDirectedRelationMap(SepCDRFile)
  val SepReCallNeighbour = UndirectedRelationMap(SepCDRFile)
  val SepBidirectionNeighbour = BidirectedRelationMap(SepCDRFile)

  val TotalCallingNeighbour = DirectedRelationMap(TotalCDRFile)
  val TotalCalledNeighbour = ReversedDirectedRelationMap(TotalCDRFile)
  val TotalReCallNeighbour = UndirectedRelationMap(TotalCDRFile)
  val TotalBidirectionNeighbour = BidirectedRelationMap(TotalCDRFile)
 /**
   * Map Message
   */
  val JanNeighbourMessage = NeighbourMessagge(JanReCallNeighbour)
  val FebNeighbourMessage = NeighbourMessagge(FebReCallNeighbour)
  val MarNeighbourMessage = NeighbourMessagge(MarReCallNeighbour)
  val AprNeighbourMessage = NeighbourMessagge(AprReCallNeighbour)
  val MayNeighbourMessage = NeighbourMessagge(MayReCallNeighbour)
  val JunNeighbourMessage = NeighbourMessagge(JunReCallNeighbour)
  val JulNeighbourMessage = NeighbourMessagge(JulReCallNeighbour)
  val AugNeighbourMessage = NeighbourMessagge(AugReCallNeighbour)
  val SepNeighbourMessage = NeighbourMessagge(SepReCallNeighbour)

  /**
   * call distribution for hours and days
   */
  val JanTimeDistribution = JanCDRFile.map(x=>{
    val slice = x.split(",")
    val time = (slice(TimePosition)+"000").toLong
    val cald = new DateUnit(time)
    val day_week = cald.getWeek()
    val hour = cald.getHour()
    ((day_week,hour),1)
  }).reduceByKey(_+_)

  val FebTimeDistribution = FebCDRFile.map(x=>{
    val slice = x.split(",")
    val time = (slice(TimePosition)+"000").toLong
    val cald = new DateUnit(time)
    val day_week = cald.getWeek()
    val hour = cald.getHour()
    ((day_week,hour),1)
  }).reduceByKey(_+_)
  //FebTimeDistribution.map(x=>x._1._1+","+x._1._2+","+x._2).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/FebTimeDstribution")

  val MarTimeDistribution = MarCDRFile.map(x=>{
    val slice = x.split(",")
    val time = (slice(TimePosition)+"000").toLong
    val cald = new DateUnit(time)
    val day_week = cald.getWeek()
    val hour = cald.getHour()
    ((day_week,hour),1)
  }).reduceByKey(_+_)
  //MarTimeDistribution.map(x=>x._1._1+","+x._1._2+","+x._2).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/MarTimeDistribution")

  /**
   * closenessPeriodicityTimeDistribution
   */
  val JanUnDirectedclosenessPeriodicityTimeDistribution = CDRTimeDistributionCloseness(UndirectedTimeDistributionRelationMap(JanCDRFile),1)
  //JanUnDirectedclosenessPeriodicityTimeDistribution.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+x._2._6+","+x._2._7).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/closenessPeriodicityTimeDistribution")
  /**
   * closenessIDF
   */
  val JanUCIDF = CDRFrequencyDurationDistributionMessage(UnDirectedFrequencyDurationDistributionRelationMap(JanCDRFile),1)
  //JanUCIDF.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanUnDirectedclosenessIDF")
    **/
  def DirectedRelationMap(file:RDD[String])={
    val DirectedMap = file.map(x => {
      val slice = x.split(",")
      if (slice(ControlPosition).equals("0")) ((slice(CallerPosition), slice(CalleePosition)), (1, slice(DurationPosition).toDouble,slice(TimePosition).toDouble,slice(TimePosition).toDouble)) else ((slice(CalleePosition), slice(CallerPosition)), (1, slice(DurationPosition).toDouble,slice(TimePosition).toDouble,slice(TimePosition).toDouble))
    }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2,min(x._3,y._3),max(x._4,y._4)))
    DirectedMap
  }
  def ReversedDirectedRelationMap(file:RDD[String])={
    val ReversedDirectedMap = file.map(x => {
      val slice = x.split(",")
      if (slice(ControlPosition).equals("0")) ((slice(CalleePosition), slice(CallerPosition)), (1, slice(DurationPosition).toDouble,slice(TimePosition).toDouble,slice(TimePosition).toDouble)) else ((slice(CallerPosition), slice(CalleePosition)), (1, slice(DurationPosition).toDouble,slice(TimePosition).toDouble,slice(TimePosition).toDouble))
    }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2,min(x._3,y._3),max(x._4,y._4)))
    ReversedDirectedMap
  }
  def UndirectedRelationMap(file:RDD[String])={
    val DirectedMap = DirectedRelationMap(file)
    val ReversedDirectedMap = ReversedDirectedRelationMap(file)
    val UndirectedMap = DirectedMap.union(ReversedDirectedMap).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2,min(x._3,y._3),max(x._4,y._4)))
    UndirectedMap
  }
  def BidirectedRelationMap(file:RDD[String])={
    val DirectedMap = DirectedRelationMap(file)
    val ReversedDirectedMap = ReversedDirectedRelationMap(file)
    val BidirectedMap = DirectedMap.join(ReversedDirectedMap).map(x=>((x._1._1,x._1._2),(x._2._1._1+x._2._2._1,x._2._1._2+x._2._2._2,min(x._2._1._3,x._2._2._3),max(x._2._1._4,x._2._2._4))))
    BidirectedMap
  }

  def DirectedTimeDistributionRelationMap(file:RDD[String])={
    val DirectedMap = file.map(x => {
      val slice = x.split(",")
      val timeArray = new ArrayBuffer[Long]()
      val timeString = slice(TimePosition)+"000"
      timeArray += timeString.toLong
      if (slice(ControlPosition).equals("0")) ((slice(CallerPosition), slice(CalleePosition)), (1, slice(DurationPosition).toDouble,slice(TimePosition).toDouble,slice(TimePosition).toDouble,timeArray)) else ((slice(CalleePosition), slice(CallerPosition)), (1, slice(DurationPosition).toDouble,slice(TimePosition).toDouble,slice(TimePosition).toDouble,timeArray))
    }).reduceByKey((x, y) => {
      val timeArray = new ArrayBuffer[Long]()
      timeArray ++= x._5
      timeArray ++= y._5
      (x._1 + y._1, x._2 + y._2,min(x._3,y._3),max(x._4,y._4),timeArray)
    })
    DirectedMap
  }
  def ReversedDirectedTimeDistributionRelationMap(file:RDD[String])={
    val ReversedDirectedMap = file.map(x => {
      val slice = x.split(",")
      val timeArray = new ArrayBuffer[Long]()
      val timeString = slice(TimePosition)+"000"
      timeArray += timeString.toLong
      if (slice(ControlPosition).equals("0")) ((slice(CalleePosition), slice(CallerPosition)), (1, slice(DurationPosition).toDouble,slice(TimePosition).toDouble,slice(TimePosition).toDouble,timeArray)) else ((slice(CallerPosition), slice(CalleePosition)), (1, slice(DurationPosition).toDouble,slice(TimePosition).toDouble,slice(TimePosition).toDouble,timeArray))
    }).reduceByKey((x, y) => {
      val timeArray = new ArrayBuffer[Long]()
      timeArray ++= x._5
      timeArray ++= y._5
      (x._1 + y._1, x._2 + y._2,min(x._3,y._3),max(x._4,y._4),timeArray)
    })
    ReversedDirectedMap
  }
  def UndirectedTimeDistributionRelationMap(file:RDD[String])={
    val DirectedMap = DirectedTimeDistributionRelationMap(file)
    val ReversedDirectedMap = ReversedDirectedTimeDistributionRelationMap(file)
    val UndirectedMap = DirectedMap.union(ReversedDirectedMap).reduceByKey((x, y) => {
      val timeArray = new ArrayBuffer[Long]()
      timeArray ++= x._5
      timeArray ++= y._5
      (x._1 + y._1, x._2 + y._2,min(x._3,y._3),max(x._4,y._4),timeArray)
    })
    UndirectedMap
  }
  def BidirectedTimeDistributionRelationMap(file:RDD[String])={
    val DirectedMap = DirectedTimeDistributionRelationMap(file)
    val ReversedDirectedMap = ReversedDirectedTimeDistributionRelationMap(file)
    val BidirectedMap = DirectedMap.join(ReversedDirectedMap).map(x=>{
      val timeArray = new ArrayBuffer[Long]()
      timeArray ++= x._2._1._5
      timeArray ++= x._2._2._5
      ((x._1._1,x._1._2),(x._2._1._1+x._2._2._1,x._2._1._2+x._2._2._2,min(x._2._1._3,x._2._2._3),max(x._2._1._4,x._2._2._4),timeArray))
    })
    BidirectedMap
  }

  def DirectedFrequencyDurationDistributionRelationMap(file:RDD[String])={
    val DirectedMap = file.map(x => {
      val slice = x.split(",")
      val timeArray = new ArrayBuffer[Long]()
      val timeString = slice(TimePosition)+"000"
      timeArray += timeString.toLong
      val cald = new DateUnit(timeString.toLong)
      val hour = cald.getHour()
      val dayWeek = cald.getWeek()-1
      val frequencyHourDistribution = new Array[Double](24)
      val durationHourDistribution = new Array[Double](24)
      val frequencyWeekDistribution = new Array[Double](7)
      val durationWeekDistribuion = new Array[Double](7)
      frequencyHourDistribution(hour) = 1.0
      frequencyWeekDistribution(dayWeek) = 1.0
      durationHourDistribution(hour) = slice(DurationPosition).toDouble
      durationWeekDistribuion(dayWeek) = slice(DurationPosition).toDouble
      if (slice(ControlPosition).equals("0")) ((slice(CallerPosition), slice(CalleePosition)), (frequencyHourDistribution,frequencyWeekDistribution,durationHourDistribution,durationWeekDistribuion,timeArray)) else ((slice(CalleePosition), slice(CallerPosition)), (frequencyHourDistribution,frequencyWeekDistribution,durationHourDistribution,durationWeekDistribuion,timeArray))
    }).reduceByKey((x, y) => {
      val frequencyHourDistribution = new Array[Double](24)
      val durationHourDistribution = new Array[Double](24)
      val frequencyWeekDistribution = new Array[Double](7)
      val durationWeekDistribuion = new Array[Double](7)
      for(i<-0 until 24){
        frequencyHourDistribution(i) = x._1(i) + y._1(i)
        durationHourDistribution(i) = x._3(i) + y._3(i)
      }
      for(i<-0 until 7){
        frequencyWeekDistribution(i) = x._2(i) + y._2(i)
        durationWeekDistribuion(i) = x._4(i) + y._4(i)
      }
      val timeArray = new ArrayBuffer[Long]()
      timeArray ++= x._5
      timeArray ++= y._5
      (frequencyHourDistribution,frequencyWeekDistribution,durationHourDistribution,durationWeekDistribuion,timeArray)
    })
    DirectedMap
  }
  def ReversedDirectedFrequencyDurationDistributionRelationMap(file:RDD[String])={
    val DirectedMap = file.map(x => {
      val slice = x.split(",")
      val timeArray = new ArrayBuffer[Long]()
      val timeString = slice(TimePosition)+"000"
      timeArray += timeString.toLong
      val cald = new DateUnit(timeString.toLong)
      val hour = cald.getHour()
      val dayWeek = cald.getWeek()-1
      val frequencyHourDistribution = new Array[Double](24)
      val durationHourDistribution = new Array[Double](24)
      val frequencyWeekDistribution = new Array[Double](7)
      val durationWeekDistribuion = new Array[Double](7)
      frequencyHourDistribution(hour) = 1.0
      frequencyWeekDistribution(dayWeek) = 1.0
      durationHourDistribution(hour) = slice(DurationPosition).toDouble
      durationWeekDistribuion(dayWeek) = slice(DurationPosition).toDouble
      if (slice(ControlPosition).equals("0")) ((slice(CalleePosition), slice(CallerPosition)), (frequencyHourDistribution,frequencyWeekDistribution,durationHourDistribution,durationWeekDistribuion,timeArray)) else ((slice(CallerPosition), slice(CalleePosition)), (frequencyHourDistribution,frequencyWeekDistribution,durationHourDistribution,durationWeekDistribuion,timeArray))
    }).reduceByKey((x, y) => {
      val frequencyHourDistribution = new Array[Double](24)
      val durationHourDistribution = new Array[Double](24)
      val frequencyWeekDistribution = new Array[Double](7)
      val durationWeekDistribuion = new Array[Double](7)
      for(i<-0 until 24){
        frequencyHourDistribution(i) = x._1(i) + y._1(i)
        durationHourDistribution(i) = x._3(i) + y._3(i)
      }
      for(i<-0 until 7){
        frequencyWeekDistribution(i) = x._2(i) + y._2(i)
        durationWeekDistribuion(i) = x._4(i) + y._4(i)
      }
      val timeArray = new ArrayBuffer[Long]()
      timeArray ++= x._5
      timeArray ++= y._5
      (frequencyHourDistribution,frequencyWeekDistribution,durationHourDistribution,durationWeekDistribuion,timeArray)
    })
    DirectedMap
  }
  def UnDirectedFrequencyDurationDistributionRelationMap(file:RDD[String])={
    val DirectedMap = DirectedFrequencyDurationDistributionRelationMap(file)
    val ReversedDirectedMap = ReversedDirectedFrequencyDurationDistributionRelationMap(file)
    val UndirectedMap = DirectedMap.union(ReversedDirectedMap).reduceByKey((x, y) => {
      val frequencyHourDistribution = new Array[Double](24)
      val durationHourDistribution = new Array[Double](24)
      val frequencyWeekDistribution = new Array[Double](7)
      val durationWeekDistribuion = new Array[Double](7)
      for(i<-0 until 24){
        frequencyHourDistribution(i) = x._1(i) + y._1(i)
        durationHourDistribution(i) = x._3(i) + y._3(i)
      }
      for(i<-0 until 7){
        frequencyWeekDistribution(i) = x._2(i) + y._2(i)
        durationWeekDistribuion(i) = x._4(i) + y._4(i)
      }
      val timeArray = new ArrayBuffer[Long]()
      timeArray ++= x._5
      timeArray ++= y._5
      (frequencyHourDistribution,frequencyWeekDistribution,durationHourDistribution,durationWeekDistribuion,timeArray)
    })
    UndirectedMap
  }
  def BiDirectedFrequencyDurationDistributionRelationMap(file:RDD[String])={
    val DirectedMap = DirectedFrequencyDurationDistributionRelationMap(file)
    val ReversedDirectedMap = ReversedDirectedFrequencyDurationDistributionRelationMap(file)
    val BidirectedMap = DirectedMap.join(ReversedDirectedMap).map(x=>{
      val frequencyHourDistribution = new Array[Double](24)
      val durationHourDistribution = new Array[Double](24)
      val frequencyWeekDistribution = new Array[Double](7)
      val durationWeekDistribuion = new Array[Double](7)
      for(i<-0 until 24){
        frequencyHourDistribution(i) = x._2._1._1(i) + x._2._2._1(i)
        durationHourDistribution(i) = x._2._1._3(i) + x._2._2._3(i)
      }
      for(i<-0 until 7){
        frequencyWeekDistribution(i) = x._2._1._2(i) + x._2._2._2(i)
        durationWeekDistribuion(i) = x._2._1._4(i) + x._2._2._4(i)
      }
      val timeArray = new ArrayBuffer[Long]()
      timeArray ++= x._2._1._5
      timeArray ++= x._2._2._5
      (x._1,(frequencyHourDistribution,frequencyWeekDistribution,durationHourDistribution,durationWeekDistribuion,timeArray))
    })
    BidirectedMap
  }
  /**
   *Edge Weight to Closeness
   */
  def NeighbourMessagge(RelationMap:RDD[((String, String), (Int, Double,Double,Double))])={
    val NeighbourMessage = RelationMap.map(x=>{
      val user = x._1._1
      val maxF = x._2._1
      val maxT = x._2._2
      val neighbourNum = 1
      val sumF = x._2._1
      val sumT = x._2._2
      val first = x._2._3
      val last = x._2._4
      val interval = (last-first)/sumF
      (user,(maxF,maxT,neighbourNum,sumF,sumT,interval,interval))
    }).reduceByKey((x,y)=>{
      val maxF = max(x._1,y._1)
      val maxT = max(x._2,y._2)
      val neighbourNum = x._3+y._3
      val sumF = x._4+y._4
      val sumT = x._5+y._5
      val maxI = max(x._6,y._6)
      val sumI = x._7+y._7
      (maxF,maxT,neighbourNum,sumF,sumT,maxI,sumI)
    })
    NeighbourMessage
  }
  def CDRCloseness(RelationMap:RDD[((String, String), (Int, Double,Double,Double))],Type:Int)={
    val NeighbourMessage = NeighbourMessagge(RelationMap)
    val CDRCloseness = RelationMap.map(x=>(x._1._1,(x._1._2,x._2._1,x._2._2,(x._2._4-x._2._3)/x._2._1))).join(NeighbourMessage).map(x=>{
      val user = x._1
      val neighbour = x._2._1._1
      val frequency = x._2._1._2.toDouble
      val duration = x._2._1._3.toDouble
      val interval = x._2._1._4
      val maxF = x._2._2._1.toDouble
      val maxT = x._2._2._2
      val neighboursNum = x._2._2._3.toDouble
      val sumF = x._2._2._4.toDouble
      val sumT = x._2._2._5
      val maxI = x._2._2._6
      val sumI = x._2._2._7
      val f = frequency/maxF
      val t = duration/maxT
      val i =interval/maxI
      val uF = sumF/(neighboursNum*maxF)
      val uT = sumT/(neighboursNum*maxT)
      val uI = sumI/(neighboursNum*maxI)
      var closeness = Closeness.closenessDistance(f, t)
      var R = Closeness.closenessDistance(uF, uT)
      if(Type == 1) {
        closeness = Closeness.closenessDistanceInterval(f, t,i)
        R = Closeness.closenessDistanceInterval(uF, uT,uI)
      }else if(Type ==2){
        closeness = Closeness.closenessInterval(f, i)
        R = Closeness.closenessInterval(uF, uI)
      }else{
        closeness = Closeness.closenessDistance(f, t)
        R = Closeness.closenessDistance(uF, uT)
      }
      ((user,neighbour),(closeness,R,closeness/R,f,t,uF,uT))
    })
    CDRCloseness.map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().map(x=>{
      val infoArray = x._2.toArray
      val l = infoArray.length
      val closenessR = new Array[Double](l)
      for(i<-0 until l){
        closenessR(i) = infoArray(i)._2._3
      }
      val sortedClosenessR = closenessR.sorted
      val result = new Array[((String,String),(Double,Double,Double,Double,Double,Double,Double,Int,Int))](l)
      for(i<-0 until l){
        val r = ((x._1,infoArray(i)._1),(infoArray(i)._2._1,infoArray(i)._2._2,infoArray(i)._2._3,infoArray(i)._2._4,infoArray(i)._2._5,infoArray(i)._2._6,infoArray(i)._2._7,sortedClosenessR.indexOf(infoArray(i)._2._3),l))
        result(i) = r
      }
      result
    }).flatMap(x=>x)
  }

  def NeighbourTimeDistributionMessagge(RelationMap:RDD[((String, String), (Int, Double,Double,Double,ArrayBuffer[Long]))])={
    val NeighbourMessage = RelationMap.map(x=>{
      val user = x._1._1
      val maxF = x._2._1
      val maxT = x._2._2
      val neighbourNum = 1
      val sumF = x._2._1
      val sumT = x._2._2
      val first = x._2._3
      val last = x._2._4
      val interval = (last-first)/sumF
      val callTime = x._2._5
      val hourDistribution = new Array[Double](24)
      val weekDistribution = new Array[Double](7)
      val callTimeLength = callTime.length
      for(i<-0 until callTimeLength){
        val date = new DateUnit(callTime(i))
        val day_week = date.getWeek()-1
        val hour = date.getHour()
        hourDistribution(hour) = hourDistribution(hour)+1.0
        weekDistribution(day_week) = weekDistribution(day_week)+1.0
      }
      (user,(maxF,maxT,neighbourNum,sumF,sumT,interval,interval,hourDistribution))
    }).reduceByKey((x,y)=>{
      val maxF = max(x._1,y._1)
      val maxT = max(x._2,y._2)
      val neighbourNum = x._3+y._3
      val sumF = x._4+y._4
      val sumT = x._5+y._5
      val maxI = max(x._6,y._6)
      val sumI = x._7+y._7
      val hourDistribution = new Array[Double](24)
      for(i<-0 until 24){
        hourDistribution(i) = x._8(i) + y._8(i)
      }
      (maxF,maxT,neighbourNum,sumF,sumT,maxI,sumI,hourDistribution)
    })
    NeighbourMessage
  }
  def CDRTimeDistributionCloseness(RelationMap:RDD[((String, String), (Int, Double,Double,Double,ArrayBuffer[Long]))],Type:Int)={
    val NeighbourMessage = NeighbourTimeDistributionMessagge(RelationMap)
    val CDRCloseness = RelationMap.map(x=>(x._1._1,(x._1._2,x._2._1,x._2._2,(x._2._4-x._2._3)/x._2._1,x._2._5))).join(NeighbourMessage).map(x=>{
      val user = x._1
      val neighbour = x._2._1._1
      val frequency = x._2._1._2.toDouble
      val duration = x._2._1._3.toDouble
      val interval = x._2._1._4
      val callTime = x._2._1._5.toArray
      val hourDistribution = new Array[Double](24)
      val weekDistribution = new Array[Double](7)
      val callTimeLength = callTime.length
      for(i<-0 until callTimeLength){
        val date = new DateUnit(callTime(i))
        val day_week = date.getWeek()-1
        val hour = date.getHour()
        hourDistribution(hour) = hourDistribution(hour)+1.0
        weekDistribution(day_week) = weekDistribution(day_week)+1.0
      }
      val maxF = x._2._2._1.toDouble
      val maxT = x._2._2._2
      val neighboursNum = x._2._2._3.toDouble
      val sumF = x._2._2._4.toDouble
      val sumT = x._2._2._5
      val maxI = x._2._2._6
      val sumI = x._2._2._7
      val userHourDistribution = x._2._2._8

      val f = frequency/maxF
      val t = duration/maxT
      val i =interval/maxI
      val uF = sumF/(neighboursNum*maxF)
      val uT = sumT/(neighboursNum*maxT)
      val uI = sumI/(neighboursNum*maxI)
      val closeness = Closeness.closenessPeriodicityTimeDistribution(0.5,callTime,hourDistribution,userHourDistribution)
      /**
      var closeness = closenessDistance(f, t)
        var R = closenessDistance(uF, uT)
        if(Type == 1) {
          closeness = closenessDistanceInterval(f, t,i)
          R = closenessDistanceInterval(uF, uT,uI)
        }else if(Type ==2){
          closeness = closenessInterval(f, i)
          R = closenessInterval(uF, uI)
        }else{
          closeness = closenessDistance(f, t)
          R = closenessDistance(uF, uT)
        }
        **/
      ((user,neighbour),(closeness,f,t,uF,uT))
    })
    CDRCloseness.map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().map(x=>{
      val infoArray = x._2.toArray
      val l = infoArray.length
      val closeness = new Array[Double](l)
      for(i<-0 until l){
        closeness(i) = infoArray(i)._2._1
      }
      val sortedCloseness = closeness.sorted
      val result = new Array[((String,String),(Double,Double,Double,Double,Double,Int,Int))](l)
      for(i<-0 until l){
        val r = ((x._1,infoArray(i)._1),(infoArray(i)._2._1,infoArray(i)._2._2,infoArray(i)._2._3,infoArray(i)._2._4,infoArray(i)._2._5,sortedCloseness.indexOf(infoArray(i)._2._1),l))
        result(i) = r
      }
      result
    }).flatMap(x=>x)
  }

  def NeightbourFrequencyDurationDistributionMessage(RelationMap:RDD[((String,String),(Array[Double],Array[Double],Array[Double],Array[Double],ArrayBuffer[Long]))])={
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
    NeighbourMessage
  }
  def CDRFrequencyDurationDistributionMessage(RelationMap:RDD[((String,String),(Array[Double],Array[Double],Array[Double],Array[Double],ArrayBuffer[Long]))],Type:Int)={
    val NeighbourMessage = NeightbourFrequencyDurationDistributionMessage(RelationMap)
    val CDRCloseness = RelationMap.map(x=>(x._1._1,(x._1._2,x._2._1,x._2._2,x._2._3,x._2._4,x._2._5))).join(NeighbourMessage).map(x=>{
      val user = x._1
      val neighbour = x._2._1._1
      val frequencyHourDistribution = x._2._1._2
      val frequencyWeekDistribution = x._2._1._3
      val durationHourDistribution = x._2._1._4
      val durationWeekDistribution = x._2._1._5
      val callTime = x._2._1._6.toArray
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
      val T = FFT.maxF(callTime,86400000)
      val closenessDistance = Closeness.closenessDistance(f,t)
      val closenessDistanceR = closenessDistance/Closeness.closenessDistance(uF,uT)
      val closenessDistanceInterval = Closeness.closenessDistanceInterval(f,t,i)
      val closenessDistanceIntervalR = closenessDistanceInterval/Closeness.closenessDistanceInterval(uF,uT,uI)
      val closenessInterval = Closeness.closenessInterval(f,i)
      val closenessIntervalR = closenessInterval/Closeness.closenessInterval(uF,uI)
      val closenessPeriodicityTimeDistribution = Closeness.closenessPeriodicityTimeDistribution(0.5,callTime,frequencyHourDistribution,userFrequencyHourDistribution)
      val closenessIDF = Closeness.closenessIDF(frequencyHourDistribution,userFrequencyHourDistribution,durationHourDistribution,userDurationHourDistribution)
      val closenessDistanceT = if(T>0){closenessDistance/T}else{closenessDistance/0.01}
      val closenessIDFT = closenessIDF*T

      ((user,neighbour),((closenessDistance,closenessDistanceR,closenessDistanceInterval,closenessDistanceIntervalR,closenessInterval,closenessIntervalR,closenessPeriodicityTimeDistribution,closenessIDF,closenessDistanceT,closenessIDFT),(f,t,i,uF,uT,uI,T)))
    })
    CDRCloseness.map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().map(x=>{
      val infoArray = x._2.toArray
      val l = infoArray.length
      val closenessDistance = new Array[Double](l)
      val closenessDistanceR = new Array[Double](l)
      val closenessDistanceInterval = new Array[Double](l)
      val closenessDistanceIntervalR = new Array[Double](l)
      val closenessInterval = new Array[Double](l)
      val closenessIntervalR = new Array[Double](l)
      val closenessPeriodicityTimeDistribution = new Array[Double](l)
      val closenessIDF = new Array[Double](l)
      val closenessDistanceT = new Array[Double](l)
      val closenessIDFT = new Array[Double](l)
      for(i<-0 until l){
        closenessDistance(i) = infoArray(i)._2._1._1
        closenessDistanceR(i) = infoArray(i)._2._1._2
        closenessDistanceInterval(i) = infoArray(i)._2._1._3
        closenessDistanceIntervalR(i) = infoArray(i)._2._1._4
        closenessInterval(i) = infoArray(i)._2._1._5
        closenessIntervalR(i) = infoArray(i)._2._1._6
        closenessPeriodicityTimeDistribution(i) = infoArray(i)._2._1._7
        closenessIDF(i) = infoArray(i)._2._1._8
        closenessDistanceT(i) = infoArray(i)._2._1._9
        closenessIDFT(i) = infoArray(i)._2._1._10
      }
      val sortedclosenessDistance = closenessDistance.sorted
      val sortedclosenessDistanceR = closenessDistanceR.sorted
      val sortedclosenessDistanceInterval = closenessDistanceInterval.sorted
      val sortedclosenessDistanceIntervalR = closenessDistanceIntervalR.sorted
      val sortedclosenessInterval = closenessInterval.sorted
      val sortedclosenessIntervalR = closenessIntervalR.sorted
      val sortedclosenessPeriodicityTimeDistribution = closenessPeriodicityTimeDistribution.sorted.reverse
      val sortedclosenessIDF = closenessIDF.sorted.reverse
      val sortedclosenessDistanceT = closenessDistanceT.sorted
      val sortedclosenessIDFT = closenessIDFT.sorted.reverse

      val result = new Array[((String,String),((Double,Double,Double,Double,Double,Double,Double,Double,Double,Double),(Double,Double,Double,Double,Double,Double,Double),(Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int)))](l)
      for(i<-0 until l){
        val rank = (sortedclosenessDistance.indexOf(infoArray(i)._2._1._1),sortedclosenessDistanceR.indexOf(infoArray(i)._2._1._2),sortedclosenessDistanceInterval.indexOf(infoArray(i)._2._1._3),sortedclosenessDistanceIntervalR.indexOf(infoArray(i)._2._1._4),sortedclosenessInterval.indexOf(infoArray(i)._2._1._5),sortedclosenessIntervalR.indexOf(infoArray(i)._2._1._6),sortedclosenessPeriodicityTimeDistribution.indexOf(infoArray(i)._2._1._7),sortedclosenessIDF.indexOf(infoArray(i)._2._1._8),sortedclosenessDistanceT.indexOf(infoArray(i)._2._1._9),sortedclosenessIDFT.indexOf(infoArray(i)._2._1._10),l)
        val r = ((x._1,infoArray(i)._1),(infoArray(i)._2._1,infoArray(i)._2._2,rank))
        result(i) = r
      }
      result
    }).flatMap(x=>x)
  }
}
