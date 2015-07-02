import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
/**
 * Created by LiuShifeng on 2014/11/21.
 */
object ClosenessCalculate{
  def ClosenessCalculate(sc:SparkContext) {
    val InterestCodeFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-Code-NanNing.csv"
    val CodePosition = 0
    val NamePosition = 1
    val ParentCodePostion = 2
    val LevelPosition = 3
    val VectorPosition = 4
    val InterestCodeFile = sc.textFile(InterestCodeFilePath)
    val InterestCodeMap = InterestCodeFile.map(x => {
      val slice = x.split(",")
      (slice(CodePosition), slice(VectorPosition))
    })

    val ServInterestBuild = new ServInterestBuild(InterestCodeMap)
    /**
     * CDR file and CDR map Build
     */
    val JanCDRFile = sc.textFile(CallMapBuild.JanCDRPath)
    val FebCDRFile = sc.textFile(CallMapBuild.FebCDRPath)
    val MarCDRFile = sc.textFile(CallMapBuild.MarCDRPath)
    val AprCDRFile = sc.textFile(CallMapBuild.AprCDRPath)
    val MayCDRFile = sc.textFile(CallMapBuild.MayCDRPath)
    val JunCDRFile = sc.textFile(CallMapBuild.JunCDRPath)
    val JulCDRFile = sc.textFile(CallMapBuild.JulCDRPath)
    val AugCDRFile = sc.textFile(CallMapBuild.AugCDRPath)
    val SepCDRFile = sc.textFile(CallMapBuild.SepCDRPath)
    val TotalCDRFile =sc.textFile(CallMapBuild.TotalCDRPath)

    /**
     * NeighbourMap
     */
    val JanCallingNeighbour = CallMapBuild.DirectedRelationMap(JanCDRFile)
    val JanCalledNeighbour = CallMapBuild.ReversedDirectedRelationMap(JanCDRFile)
    val JanReCallNeighbour = CallMapBuild.UndirectedRelationMap(JanCDRFile)
    val JanBidirectionNeighbour = CallMapBuild.BidirectedRelationMap(JanCDRFile)

    val FebCallingNeighbour = CallMapBuild.DirectedRelationMap(FebCDRFile)
    val FebCalledNeighbour = CallMapBuild.ReversedDirectedRelationMap(FebCDRFile)
    val FebReCallNeighbour = CallMapBuild.UndirectedRelationMap(FebCDRFile)
    val FebBidirectionNeighbour = CallMapBuild.BidirectedRelationMap(FebCDRFile)

    val MarCallingNeighbour = CallMapBuild.DirectedRelationMap(MarCDRFile)
    val MarCalledNeighbour = CallMapBuild.ReversedDirectedRelationMap(MarCDRFile)
    val MarReCallNeighbour = CallMapBuild.UndirectedRelationMap(MarCDRFile)
    val MarBidirectionNeighbour = CallMapBuild.BidirectedRelationMap(MarCDRFile)

    val AprCallingNeighbour = CallMapBuild.DirectedRelationMap(AprCDRFile)
    val AprCalledNeighbour = CallMapBuild.ReversedDirectedRelationMap(AprCDRFile)
    val AprReCallNeighbour = CallMapBuild.UndirectedRelationMap(AprCDRFile)
    val AprBidirectionNeighbour = CallMapBuild.BidirectedRelationMap(AprCDRFile)

    val MayCallingNeighbour = CallMapBuild.DirectedRelationMap(MayCDRFile)
    val MayCalledNeighbour = CallMapBuild.ReversedDirectedRelationMap(MayCDRFile)
    val MayReCallNeighbour = CallMapBuild.UndirectedRelationMap(MayCDRFile)
    val MayBidirectionNeighbour = CallMapBuild.BidirectedRelationMap(MayCDRFile)

    val JunCallingNeighbour = CallMapBuild.DirectedRelationMap(JunCDRFile)
    val JunCalledNeighbour = CallMapBuild.ReversedDirectedRelationMap(JunCDRFile)
    val JunReCallNeighbour = CallMapBuild.UndirectedRelationMap(JunCDRFile)
    val JunBidirectionNeighbour = CallMapBuild.BidirectedRelationMap(JunCDRFile)

    val JulCallingNeighbour = CallMapBuild.DirectedRelationMap(JulCDRFile)
    val JulCalledNeighbour = CallMapBuild.ReversedDirectedRelationMap(JulCDRFile)
    val JulReCallNeighbour = CallMapBuild.UndirectedRelationMap(JulCDRFile)
    val JulBidirectionNeighbour = CallMapBuild.BidirectedRelationMap(JulCDRFile)

    val AugCallingNeighbour = CallMapBuild.DirectedRelationMap(AugCDRFile)
    val AugCalledNeighbour = CallMapBuild.ReversedDirectedRelationMap(AugCDRFile)
    val AugReCallNeighbour = CallMapBuild.UndirectedRelationMap(AugCDRFile)
    val AugBidirectionNeighbour = CallMapBuild.BidirectedRelationMap(AugCDRFile)

    val SepCallingNeighbour = CallMapBuild.DirectedRelationMap(SepCDRFile)
    val SepCalledNeighbour = CallMapBuild.ReversedDirectedRelationMap(SepCDRFile)
    val SepReCallNeighbour = CallMapBuild.UndirectedRelationMap(SepCDRFile)
    val SepBidirectionNeighbour = CallMapBuild.BidirectedRelationMap(SepCDRFile)

    val TotalCallingNeighbour = CallMapBuild.DirectedRelationMap(TotalCDRFile)
    val TotalCalledNeighbour = CallMapBuild.ReversedDirectedRelationMap(TotalCDRFile)
    val TotalReCallNeighbour = CallMapBuild.UndirectedRelationMap(TotalCDRFile)
    val TotalBidirectionNeighbour = CallMapBuild.BidirectedRelationMap(TotalCDRFile)
    /**
     * Map Message
     */
    val JanNeighbourMessage = CallMapBuild.NeighbourMessagge(JanReCallNeighbour)
    val FebNeighbourMessage = CallMapBuild.NeighbourMessagge(FebReCallNeighbour)
    val MarNeighbourMessage = CallMapBuild.NeighbourMessagge(MarReCallNeighbour)
    val AprNeighbourMessage = CallMapBuild.NeighbourMessagge(AprReCallNeighbour)
    val MayNeighbourMessage = CallMapBuild.NeighbourMessagge(MayReCallNeighbour)
    val JunNeighbourMessage = CallMapBuild.NeighbourMessagge(JunReCallNeighbour)
    val JulNeighbourMessage = CallMapBuild.NeighbourMessagge(JulReCallNeighbour)
    val AugNeighbourMessage = CallMapBuild.NeighbourMessagge(AugReCallNeighbour)
    val SepNeighbourMessage = CallMapBuild.NeighbourMessagge(SepReCallNeighbour)

    val JanDirectedClosenesswithInterval = CallMapBuild.CDRCloseness(JanCallingNeighbour,1)
    //JanDirectedClosenesswithInterval.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanDirectedClosenesswithInterval")
    val JanDirectedClosenessInterval = CallMapBuild.CDRCloseness(JanCallingNeighbour,2)
    //JanDirectedClosenessInterval.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanDirectedClosenessInterval")

    val JanBiDirectedCloseness = CallMapBuild.CDRCloseness(JanBidirectionNeighbour,0)
    //JanBiDirectedCloseness.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanBiDirectedCloseness")
    val JanBiDirectedClosenesswithInterval = CallMapBuild.CDRCloseness(JanBidirectionNeighbour,1)
    //JanBiDirectedClosenesswithInterval.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanBiDirectedClosenesswithInterval")
    val JanBiDirectedClosenessInterval = CallMapBuild.CDRCloseness(JanBidirectionNeighbour,2)
    //JanBiDirectedClosenessInterval.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanBiDirectedClosenessInterval")

    /**
     * file build
     */
    val JanServInterestFile = sc.textFile(ServInterestBuild.JanServInterestPath)
    val FebServInterestFile = sc.textFile(ServInterestBuild.FebServInterestPath)
    val MarServInterestFile = sc.textFile(ServInterestBuild.MarServInterestPath)
    val AprServInterestFile = sc.textFile(ServInterestBuild.AprServInterestPath)
    val JunServInterestFile = sc.textFile(ServInterestBuild.JunServInterestPath)
    val JulServInterestFile = sc.textFile(ServInterestBuild.JulServInterestPath)
    val AugServInterestFile = sc.textFile(ServInterestBuild.AugServInterestPath)
    val SepServInterestFile = sc.textFile(ServInterestBuild.SepServInterestPath)

    /**
     * Interest 643 Vector
     */
    val JanStatisticInterestFrequence = ServInterestBuild.StatisticInterestFrequence(JanServInterestFile)
    val FebStatisticInterestFrequence = ServInterestBuild.StatisticInterestFrequence(FebServInterestFile)
    val MarStatisticInterestFrequence = ServInterestBuild.StatisticInterestFrequence(MarServInterestFile)
    val AprStatisticInterestFrequence = ServInterestBuild.StatisticInterestFrequence(AprServInterestFile)
    val JunStatisticInterestFrequence = ServInterestBuild.StatisticInterestFrequence(JunServInterestFile)
    val JulStatisticInterestFrequence = ServInterestBuild.StatisticInterestFrequence(JulServInterestFile)
    val AugStatisticInterestFrequence = ServInterestBuild.StatisticInterestFrequence(AugServInterestFile)
    val SepStatisticInterestFrequence = ServInterestBuild.StatisticInterestFrequence(SepServInterestFile)
    /**
     * INterest Merged Vector
     */
    val JanServTotalMergedInterestFrequence = ServInterestBuild.MergedInterestFrequence(JanServInterestFile)
    val FebServTotalMergedInterestFrequence = ServInterestBuild.MergedInterestFrequence(FebServInterestFile)
    val MarServTotalMergedInterestFrequence = ServInterestBuild.MergedInterestFrequence(MarServInterestFile)
    val AprServTotalMergedInterestFrequence = ServInterestBuild.MergedInterestFrequence(AprServInterestFile)
    val JunServTotalMergedInterestFrequence = ServInterestBuild.MergedInterestFrequence(JunServInterestFile)
    val JulServTotalMergedInterestFrequence = ServInterestBuild.MergedInterestFrequence(JulServInterestFile)
    val AugServTotalMergedInterestFrequence = ServInterestBuild.MergedInterestFrequence(AugServInterestFile)
    val SepServTotalMergedInterestFrequence = ServInterestBuild.MergedInterestFrequence(SepServInterestFile)
    /**
     * Similarity Calculate
     */
    val JanTotalInterestFrequencyCDRMap = JanReCallNeighbour.map(x=>(x._1._1,(x._1._2,x._2))).join(JanStatisticInterestFrequence).map(x=>(x._2._1._1,(x._1,x._2._1._2,x._2._2))).join(JanStatisticInterestFrequence).map(x=>((x._2._1._1,x._1),(x._2._1._2._1,x._2._1._2._2,x._2._1._3,x._2._2)))
    val JanTotalInterestFrequencyCDRHomo = JanTotalInterestFrequencyCDRMap.map(x=>{
      val Pre = new Array[Double](642)
      val Cur = new Array[Double](642)
      for(i<-0 until 643){
        if(i<23){
          Pre(i) = x._2._3(i)
          Cur(i) = x._2._4(i)
        }else if(i>23){
          Pre(i-1) = x._2._3(i)
          Cur(i-1) = x._2._4(i)
        }
      }
      val distance = Similarity.AdjustedCosineSimilarity(x._2._3,x._2._4)
      (x._1,(x._2._1,x._2._2,distance))
    })
    val JanTotalInterestFrequencyCloseNess = JanTotalInterestFrequencyCDRHomo.map(x=>(x._1._1,(x._1._2,x._2))).join(JanNeighbourMessage).map(x=>{
      val user = x._1
      val neighbour = x._2._1._1
      val similarity = x._2._1._2._3
      val frequency = x._2._1._2._1.toDouble
      val duration = x._2._1._2._2
      val maxF = x._2._2._1.toDouble
      val maxT = x._2._2._2
      val neighboursNum = x._2._2._3.toDouble
      val sumF = x._2._2._4.toDouble
      val sumT = x._2._2._5
      val f = frequency/maxF
      val t = duration/maxT
      val uF = sumF/(neighboursNum*maxF)
      val uT = sumT/(neighboursNum*maxT)
      val closeness = Closeness.closenessDistance(f,t)
      val R = Closeness.closenessDistance(uF,uT)
      (user,(duration/maxT,R,similarity))
    })
    //JanTotalInterestFrequencyCloseNess.map(x=>x._2._1+","+x._2._2+","+x._2._3).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanTotalInterestFrequencyCloseNess_643_duration")

    /**
     *Calculate closeness for total cdr node
     */
    val JanTotalCDRCloseness = JanReCallNeighbour.map(x=>(x._1._1,(x._1._2,x._2))).join(JanNeighbourMessage).map(x=>{
      val user = x._1
      val neighbour = x._2._1._1
      val frequency = x._2._1._2._1.toDouble
      val duration = x._2._1._2._2
      val maxF = x._2._2._1.toDouble
      val maxT = x._2._2._2
      val neighboursNum = x._2._2._3.toDouble
      val sumF = x._2._2._4.toDouble
      val sumT = x._2._2._5
      val f = frequency/maxF
      val t = duration/maxT
      val uF = sumF/(neighboursNum*maxF)
      val uT = sumT/(neighboursNum*maxT)
      val closeness = Closeness.closenessDistance(f,t)
      val R = Closeness.closenessDistance(uF,uT)
      ((user,neighbour),(closeness,R,closeness/R,f,t,uF,uT))
    })
    //JanTotalCDRCloseness.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanTotalCDRCloseness")

  }
}

