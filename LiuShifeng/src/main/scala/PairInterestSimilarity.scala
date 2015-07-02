/**
 * Created by LiuShifeng on 2014/12/20.
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.math._
object PairInterestSimilarity{
  def PairInterestSimilarity (sc:SparkContext) {
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


    val JanTotalInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(JanReCallNeighbour,JanStatisticInterestFrequence,0)
    val FebTotalInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(FebReCallNeighbour,FebStatisticInterestFrequence,0)
    val MarTotalInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(MarReCallNeighbour,MarStatisticInterestFrequence,0)
    val AprTotalInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(AprReCallNeighbour,AprStatisticInterestFrequence,0)
    val JunTotalInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(JunReCallNeighbour,JunStatisticInterestFrequence,0)
    val JulTotalInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(JulReCallNeighbour,JulStatisticInterestFrequence,0)
    val AugTotalInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(AugReCallNeighbour,AugStatisticInterestFrequence,0)
    val SepTotalInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(SepReCallNeighbour,SepStatisticInterestFrequence,0)

    val JanBiDirectionTotalInterestFrequencyCDRHomo = ServInterestBuild.Interest28Sim(JanBidirectionNeighbour,JanStatisticInterestFrequence,0)
    val FebBiDirectionTotalInterestFrequencyCDRHomo = ServInterestBuild.Interest28Sim(FebBidirectionNeighbour,FebStatisticInterestFrequence,0)
    val MarBiDirectionTotalInterestFrequencyCDRHomo = ServInterestBuild.Interest28Sim(MarBidirectionNeighbour,MarStatisticInterestFrequence,0)
    val AprBiDirectionTotalInterestFrequencyCDRHomo = ServInterestBuild.Interest28Sim(AprBidirectionNeighbour,AprStatisticInterestFrequence,0)
    val JunBiDirectionTotalInterestFrequencyCDRHomo = ServInterestBuild.Interest28Sim(JunBidirectionNeighbour,JunStatisticInterestFrequence,0)
    val JulBiDirectionTotalInterestFrequencyCDRHomo = ServInterestBuild.Interest28Sim(JulBidirectionNeighbour,JulStatisticInterestFrequence,0)
    val AugBiDirectionTotalInterestFrequencyCDRHomo = ServInterestBuild.Interest28Sim(AugBidirectionNeighbour,AugStatisticInterestFrequence,0)
    val SepBiDirectionTotalInterestFrequencyCDRHomo = ServInterestBuild.Interest28Sim(SepBidirectionNeighbour,SepStatisticInterestFrequence,0)

    /**
     * KZAdjustedCosine
     */
    val JanDirectedStatisticInterestFrequnencyKZHomo = ServInterestBuild.InterestHomo(JanCallingNeighbour,JanStatisticInterestFrequence,1)
    //JanDirectedStatisticInterestFrequnencyKZHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanDirectedStatisticInterestFrequnencyKZHomo")
    val JanUnDirectedStatisticInterestFrequnencyKZHomo = ServInterestBuild.InterestHomo(JanReCallNeighbour,JanStatisticInterestFrequence,1)
    //JanUnDirectedStatisticInterestFrequnencyKZHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4)."hdfs://dell01:12306/user/tele/LiuShifeng/JanUnDirectedStatisticInterestFrequnencyKZHomo")
    val JanBiDirectedStatisticInterestFrequnencyKZHomo = ServInterestBuild.InterestHomo(JanBidirectionNeighbour,JanStatisticInterestFrequence,1)
    //JanBiDirectedStatisticInterestFrequnencyKZHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanBiDirectedStatisticInterestFrequnencyKZHomo")
    /**
     * NeighbourPair Interest Similarity with merged frenquency which is directly calculated from the file(with merge)
     */
    val JanTotalMergedInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(JanReCallNeighbour,JanServTotalMergedInterestFrequence,0)
    //    JanTotalMergedInterestFrequencyCDRHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanTotalMergedInterestFrequencyCDRHomo")
    val FebTotalMergedInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(FebReCallNeighbour,FebServTotalMergedInterestFrequence,0)
    //    FebTotalMergedInterestFrequencyCDRHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/FebTotalMergedInterestFrequencyCDRHomo")
    val MarTotalMergedInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(MarReCallNeighbour,MarServTotalMergedInterestFrequence,0)
    //    MarTotalMergedInterestFrequencyCDRHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/MarTotalMergedInterestFrequencyCDRHomo")
    val AprTotalMergedInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(AprReCallNeighbour,AprServTotalMergedInterestFrequence,0)
    //    AprTotalMergedInterestFrequencyCDRHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/AprTotalMergedInterestFrequencyCDRHomo")
    val JunTotalMergedInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(JunReCallNeighbour,JunServTotalMergedInterestFrequence,0)
    //    JunTotalMergedInterestFrequencyCDRHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JunTotalMergedInterestFrequencyCDRHomo")
    val JulTotalMergedInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(JulReCallNeighbour,JulServTotalMergedInterestFrequence,0)
    //    JulTotalMergedInterestFrequencyCDRHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JulTotalMergedInterestFrequencyCDRHomo")
    val AugTotalMergedInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(AugReCallNeighbour,AugServTotalMergedInterestFrequence,0)
    //    AugTotalMergedInterestFrequencyCDRHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/AugTotalMergedInterestFrequencyCDRHomo")
    val SepTotalMergedInterestFrequencyCDRHomo = ServInterestBuild.InterestHomo(SepReCallNeighbour,SepServTotalMergedInterestFrequence,0)
    //    SepTotalMergedInterestFrequencyCDRHomo.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/SepTotalMergedInterestFrequencyCDRHomo")

    /**
     * get the similarity together with closeness
     */
    //val JanClosenesswithSimilarity = JanTotalMergedInterestFrequencyCDRHomo.join(JanTotalCDRCloseness)
    //JanClosenesswithSimilarity.map(x=>x._1._1+","+x._1._2+","+x._2._1._1+","+x._2._1._2+","+x._2._1._3+","+x._2._1._4+","+x._2._2._1+","+x._2._2._2+","+x._2._2._3+","+x._2._2._4+","+x._2._2._5+","+x._2._2._6+","+x._2._2._7).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanClosenesswithSimilarity")
    /**
     * Bidirected_ClosenesswithInterval_StatisticInterestFrequence_28Similarity
     */
    val JanBCI = CallMapBuild.CDRCloseness(JanBidirectionNeighbour,1)
    val JanBSFA = ServInterestBuild.Interest28Sim(JanBidirectionNeighbour,JanStatisticInterestFrequence,0)
    val JanBCISFA = JanBSFA.join(JanBCI)
    /**JanBCISFA.map(x=>{
      var out = x._1._1+","+x._1._2
      for(i<-0 until 28){
        out = out + "," +x._2._1(i)
      }
      out = out + "," + x._2._2._1 + "," + x._2._2._2 + "," + x._2._2._3 + "," + x._2._2._4 + "," + x._2._2._5 + "," + x._2._2._6 + "," + x._2._2._7+ ","+x._2._2._8 + "," + x._2._2._9
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanBCISFA")**/
    val FebBCI = CallMapBuild.CDRCloseness(FebBidirectionNeighbour,1)
    val FebBSFA = ServInterestBuild.Interest28Sim(FebBidirectionNeighbour,FebStatisticInterestFrequence,0)
    val FebBCISFA = FebBSFA.join(FebBCI)
    /**FebBCISFA.map(x=>{
      var out = x._1._1+","+x._1._2
      for(i<-0 until 28){
        out = out + "," +x._2._1(i)
      }
      out = out + "," + x._2._2._1 + "," + x._2._2._2 + "," + x._2._2._3 + "," + x._2._2._4 + "," + x._2._2._5 + "," + x._2._2._6 + "," + x._2._2._7+ ","+x._2._2._8 + "," + x._2._2._9
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/FebBCISFA")**/

    val MarBCI = CallMapBuild.CDRCloseness(MarBidirectionNeighbour,1)
    val MarBSFA = ServInterestBuild.Interest28Sim(MarBidirectionNeighbour,MarStatisticInterestFrequence,0)
    val MarBCISFA = MarBSFA.join(MarBCI)
    /**MarBCISFA.map(x=>{
      var out = x._1._1+","+x._1._2
      for(i<-0 until 28){
        out = out + "," +x._2._1(i)
      }
      out = out + "," + x._2._2._1 + "," + x._2._2._2 + "," + x._2._2._3 + "," + x._2._2._4 + "," + x._2._2._5 + "," + x._2._2._6 + "," + x._2._2._7+ ","+x._2._2._8 + "," + x._2._2._9
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/MarBCISFA")**/

    val AprBCI = CallMapBuild.CDRCloseness(AprBidirectionNeighbour,1)
    val AprBSFA = ServInterestBuild.Interest28Sim(AprBidirectionNeighbour,AprStatisticInterestFrequence,0)
    val AprBCISFA = AprBSFA.join(AprBCI)
    /**AprBCISFA.map(x=>{
      var out = x._1._1+","+x._1._2
      for(i<-0 until 28){
        out = out + "," +x._2._1(i)
      }
      out = out + "," + x._2._2._1 + "," + x._2._2._2 + "," + x._2._2._3 + "," + x._2._2._4 + "," + x._2._2._5 + "," + x._2._2._6 + "," + x._2._2._7+ ","+x._2._2._8 + "," + x._2._2._9
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/AprBCISFA")**/

    val JunBCI = CallMapBuild.CDRCloseness(JunBidirectionNeighbour,1)
    val JunBSFA = ServInterestBuild.Interest28Sim(JunBidirectionNeighbour,JunStatisticInterestFrequence,0)
    val JunBCISFA = JunBSFA.join(JunBCI)
    /**JunBCISFA.map(x=>{
      var out = x._1._1+","+x._1._2
      for(i<-0 until 28){
        out = out + "," +x._2._1(i)
      }
      out = out + "," + x._2._2._1 + "," + x._2._2._2 + "," + x._2._2._3 + "," + x._2._2._4 + "," + x._2._2._5 + "," + x._2._2._6 + "," + x._2._2._7+ ","+x._2._2._8 + "," + x._2._2._9
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JunBCISFA")**/

    val JulBCI = CallMapBuild.CDRCloseness(JulBidirectionNeighbour,1)
    val JulBSFA = ServInterestBuild.Interest28Sim(JulBidirectionNeighbour,JulStatisticInterestFrequence,0)
    val JulBCISFA = JulBSFA.join(JulBCI)
    /**JulBCISFA.map(x=>{
      var out = x._1._1+","+x._1._2
      for(i<-0 until 28){
        out = out + "," +x._2._1(i)
      }
      out = out + "," + x._2._2._1 + "," + x._2._2._2 + "," + x._2._2._3 + "," + x._2._2._4 + "," + x._2._2._5 + "," + x._2._2._6 + "," + x._2._2._7+ ","+x._2._2._8 + "," + x._2._2._9
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JulBCISFA")**/

    val AugBCI = CallMapBuild.CDRCloseness(AugBidirectionNeighbour,1)
    val AugBSFA = ServInterestBuild.Interest28Sim(AugBidirectionNeighbour,AugStatisticInterestFrequence,0)
    val AugBCISFA = AugBSFA.join(AugBCI)
    /**AugBCISFA.map(x=>{
      var out = x._1._1+","+x._1._2
      for(i<-0 until 28){
        out = out + "," +x._2._1(i)
      }
      out = out + "," + x._2._2._1 + "," + x._2._2._2 + "," + x._2._2._3 + "," + x._2._2._4 + "," + x._2._2._5 + "," + x._2._2._6 + "," + x._2._2._7+ ","+x._2._2._8 + "," + x._2._2._9
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/AugBCISFA")**/

    val SepBCI = CallMapBuild.CDRCloseness(SepBidirectionNeighbour,1)
    val SepBSFA = ServInterestBuild.Interest28Sim(SepBidirectionNeighbour,SepStatisticInterestFrequence,0)
    val SepBCISFA = SepBSFA.join(SepBCI)
    /**SepBCISFA.map(x=>{
      var out = x._1._1+","+x._1._2
      for(i<-0 until 28){
        out = out + "," +x._2._1(i)
      }
      out = out + "," + x._2._2._1 + "," + x._2._2._2 + "," + x._2._2._3 + "," + x._2._2._4 + "," + x._2._2._5 + "," + x._2._2._6 + "," + x._2._2._7+ ","+x._2._2._8 + "," + x._2._2._9
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/SepBCISFA")**/

    /**
     * Undirected_closenessPeriodicityTimeDistribution_StatisticInterestFrequence_28Similarity
     */
    val JanUCT = CallMapBuild.CDRTimeDistributionCloseness(CallMapBuild.UndirectedTimeDistributionRelationMap(JanCDRFile),1)
    val JanUSFA = ServInterestBuild.Interest28Sim(CallMapBuild.UndirectedTimeDistributionRelationMap(JanCDRFile).map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4))),JanStatisticInterestFrequence,0)
    val JanUCTSFA = JanUSFA.join(JanUCT)
    /**JanUCTSFA.map(x=>{
      var out = x._1._1+","+x._1._2
      for(i<-0 until 28){
        out = out + "," +x._2._1(i)
      }
      out = out + "," + x._2._2._1 + "," + x._2._2._2 + "," + x._2._2._3 + "," + x._2._2._4 + "," + x._2._2._5 + "," + x._2._2._6 + "," + x._2._2._7
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanUCTSFA")**/

    /**
     * Undirected_closenessIDF_StatisticInterestFrequence_28Similarity
     */
    val JanUCIDF = CallMapBuild.CDRFrequencyDurationDistributionMessage(CallMapBuild.UnDirectedFrequencyDurationDistributionRelationMap(JanCDRFile),1)
    val JanUSFAT = ServInterestBuild.Interest28SimFrequencyDurationDistribution(CallMapBuild.UnDirectedFrequencyDurationDistributionRelationMap(JanCDRFile).map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,x._2._5.toArray))),JanStatisticInterestFrequence,0)
    val JanUCIDFSFA = JanUSFAT.join(JanUCIDF)
    /**JanUCIDFSFA.map(x=>{
      var out = x._1._1+","+x._1._2
      for(i<-0 until 28){
        out = out + "," +x._2._1._2(i)
      }
      out = out + "," + x._2._2._1 + "," + x._2._2._2 + "," + x._2._2._3 + "," + x._2._2._4 + "," + x._2._2._5 + "," + x._2._2._6 + "," + x._2._2._7
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanUCIDFSFA")**/


    val JanServInterestIDF = ServInterestBuild.Idf4Interest(JanServInterestFile)
    val FebServInterestIDF = ServInterestBuild.Idf4Interest(FebServInterestFile)
    val MarServInterestIDF = ServInterestBuild.Idf4Interest(MarServInterestFile)
    val AprServInterestIDF = ServInterestBuild.Idf4Interest(AprServInterestFile)
    val JunServInterestIDF = ServInterestBuild.Idf4Interest(JunServInterestFile)
    val JulServInterestIDF = ServInterestBuild.Idf4Interest(JulServInterestFile)
    val AugServInterestIDF = ServInterestBuild.Idf4Interest(AugServInterestFile)
    val SepServInterestIDF = ServInterestBuild.Idf4Interest(SepServInterestFile)
    /**
     * the time snapshots that Internet Visit Edges have shown up
     */
    val JanTotalMergedInterestFrequencyCDRHomoSnapshot = JanTotalMergedInterestFrequencyCDRHomo.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,1)))
    val FebTotalMergedInterestFrequencyCDRHomoSnapshot = FebTotalMergedInterestFrequencyCDRHomo.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,2)))
    val MarTotalMergedInterestFrequencyCDRHomoSnapshot = MarTotalMergedInterestFrequencyCDRHomo.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,3)))
    val AprTotalMergedInterestFrequencyCDRHomoSnapshot = AprTotalMergedInterestFrequencyCDRHomo.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,4)))
    val JunTotalMergedInterestFrequencyCDRHomoSnapshot = JunTotalMergedInterestFrequencyCDRHomo.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,6)))
    val JulTotalMergedInterestFrequencyCDRHomoSnapshot = JulTotalMergedInterestFrequencyCDRHomo.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,7)))
    val AugTotalMergedInterestFrequencyCDRHomoSnapshot = AugTotalMergedInterestFrequencyCDRHomo.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,8)))
    val SepTotalMergedInterestFrequencyCDRHomoSnapshot = SepTotalMergedInterestFrequencyCDRHomo.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,9)))

    val TotalMergedInterestFrequencyCDRHomoSnapshot = JanTotalMergedInterestFrequencyCDRHomoSnapshot.union(FebTotalMergedInterestFrequencyCDRHomoSnapshot).union(MarTotalMergedInterestFrequencyCDRHomoSnapshot).union(AprTotalMergedInterestFrequencyCDRHomoSnapshot).union(JunTotalMergedInterestFrequencyCDRHomoSnapshot).union(JulTotalMergedInterestFrequencyCDRHomoSnapshot).union(AugTotalMergedInterestFrequencyCDRHomoSnapshot).union(SepTotalMergedInterestFrequencyCDRHomoSnapshot).groupByKey().map(x=>{
      val SnapShot = new Array[(Double,Double,Double,Double)](9)
      x._2.foreach(y=>SnapShot(y._5-1) = (y._1,y._2,y._3,y._4))
      (x._1,SnapShot)
    })
    val newEdges = TotalMergedInterestFrequencyCDRHomoSnapshot.filter(x=>{
      val snapshot = x._2
      val l = 9
      var mark = false
      var p = -1
      var changable = true
      for(i<-0 until 9 if i!=4){
        if(snapshot(i) == null && changable) {
          p = i
        } else
          changable = false
      }
      if(p != -1)
        mark = true
      mark
    })

    val firstMonth = newEdges.map(x=>{
      var p = -1
      var changable = true
      for(i<-0 until 9 if i != 4){
        if(x._2(i) != null && changable){
          p = i
          changable = false
        }
      }
      (x._1,x._2(p))
    })
    //    firstMonth.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/firstMonth")

    val firstVariation = newEdges.map(x=>{
      var p = -1
      var changable = true
      var v = -1
      for(i<-0 until 9 if i != 4){
        if(x._2(i) != null && changable){
          p = i
          changable = false
        }
      }
      changable = true
      for(i<-(p+1) until 9 if i != 4){
        if(x._2(i) != null && changable){
          v = i
          changable = false
        }
      }
      if(v != -1) {
        val varation = (x._2(v)._1 - x._2(p)._1, x._2(v)._2 - x._2(p)._2, x._2(v)._3 - x._2(p)._3, x._2(v)._4 - x._2(p)._4)
        (x._1, varation)
      }else
        (x._1,(0.0,0.0,0.0,0.0))

    })
    //    firstVariation.filter(x=>(x._2._1 != 0.0 && x._2._2 != 0.0 && x._2._3 != 0.0 && x._2._4 != 0.0)).map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/firstVariation")

    val firstVariationforTotal = TotalMergedInterestFrequencyCDRHomoSnapshot.map(x=>{
      var p = -1
      var changable = true
      var v = -1
      for(i<-0 until 9 if i != 4){
        if(x._2(i) != null && changable){
          p = i
          changable = false
        }
      }
      changable = true
      for(i<-(p+1) until 9 if i != 4){
        if(x._2(i) != null && changable){
          v = i
          changable = false
        }
      }
      if(v != -1) {
        val varation = (x._2(v)._1 - x._2(p)._1, x._2(v)._2 - x._2(p)._2, x._2(v)._3 - x._2(p)._3, x._2(v)._4 - x._2(p)._4)
        (x._1, varation)
      }else
        (x._1,(0.0,0.0,0.0,0.0))

    })
    //    firstVariationforTotal.filter(x=>(x._2._1 != 0.0 && x._2._2 != 0.0 && x._2._3 != 0.0 && x._2._4 != 0.0)).map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/firstVariationforTotal")
    /**
     * each interest's idf from Jan to Sep
     */
    val JanServInterestIDFT = JanServInterestIDF.map(x=>(x._1,(x._2._1,x._2._2,1)))
    val FebServInterestIDFT = FebServInterestIDF.map(x=>(x._1,(x._2._1,x._2._2,2)))
    val MarServInterestIDFT = MarServInterestIDF.map(x=>(x._1,(x._2._1,x._2._2,3)))
    val AprServInterestIDFT = AprServInterestIDF.map(x=>(x._1,(x._2._1,x._2._2,4)))
    val JunServInterestIDFT = JunServInterestIDF.map(x=>(x._1,(x._2._1,x._2._2,6)))
    val JulServInterestIDFT = JulServInterestIDF.map(x=>(x._1,(x._2._1,x._2._2,7)))
    val AugServInterestIDFT = AugServInterestIDF.map(x=>(x._1,(x._2._1,x._2._2,8)))
    val SepServInterestIDFT = SepServInterestIDF.map(x=>(x._1,(x._2._1,x._2._2,9)))
    val ServInterestIDF = JanServInterestIDFT.union(FebServInterestIDFT).union(MarServInterestIDFT).union(AprServInterestIDFT).union(JunServInterestIDFT).union(JulServInterestIDFT).union(AugServInterestIDFT).union(SepServInterestIDFT).groupByKey().map(x=>{
      val SnapShot = new Array[(Double,Int)](9)
      x._2.foreach(y=>SnapShot(y._3-1) = (y._1,y._2))
      (x._1,SnapShot)
    })
    /**ServInterestIDF.map(x=>{
      var vectorPosition = 0
      var num = 0
      var sum = 0.0
      var idf = ""
      for(i<-0 until 9 ){
        if(x._2(i) != null){
          num = num + 1
          sum = sum + x._2(i)._1
          vectorPosition = x._2(i)._2
        }
      }
      val average = sum/num
      for(i<-0 until 9){
        if(x._2(i) != null){
          idf = idf + "," + x._2(i)._1
        }else
          idf = idf + "," + average
      }
      x._1+","+vectorPosition+idf
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/ServInterestIDF")**/

      /**
       * each edge's call messages from Jan to Sep,if no call exists then the statistic is zero
       */
      //directed
      val JanDirectedMap = JanCallingNeighbour.map(x =>(x._1, (x._2._1, x._2._2, 1)))
      val FebDirectedMap = FebCallingNeighbour.map(x =>(x._1, (x._2._1, x._2._2, 2)))
      val MarDirectedMap = MarCallingNeighbour.map(x =>(x._1, (x._2._1, x._2._2, 3)))
      val AprDirectedMap = AprCallingNeighbour.map(x =>(x._1, (x._2._1, x._2._2, 4)))
      val MayDirectedMap = MayCallingNeighbour.map(x =>(x._1, (x._2._1, x._2._2, 5)))
      val JunDirectedMap = JunCallingNeighbour.map(x =>(x._1, (x._2._1, x._2._2, 6)))
      val JulDirectedMap = JulCallingNeighbour.map(x =>(x._1, (x._2._1, x._2._2, 7)))
      val AugDirectedMap = AugCallingNeighbour.map(x =>(x._1, (x._2._1, x._2._2, 8)))
      val SepDirectedMap = SepCallingNeighbour.map(x =>(x._1, (x._2._1, x._2._2, 9)))

      val JanDirectedMessage = JanDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 1)))

      val FebDirectedMessage = FebDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 2)))

      val MarDirectedMessage = MarDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 3)))

      val AprDirectedMessage = AprDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 4)))

      val MayDirectedMessage = MayDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 5)))

      val JunDirectedMessage = JunDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 6)))

      val JulDirectedMessage = JulDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 7)))

      val AugDirectedMessage = AugDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 8)))

      val SepDirectedMessage = SepDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 9)))

      val DirectedCallMap = JanDirectedMap.union(FebDirectedMap).union(MarDirectedMap).union(AprDirectedMap).union(MayDirectedMap).union(JunDirectedMap).union(JulDirectedMap).union(AugDirectedMap).union(SepDirectedMap).groupByKey().map(x => {
        val SnapShot = new Array[(Int, Double)](9)
        x._2.foreach(y => SnapShot(y._3 - 1) = (y._1, y._2))
        for (i <- 0 until 9) {
          if (SnapShot(i) == null)
            SnapShot(i) = (0, 0.0)
        }
        (x._1, SnapShot)
      })
      val DirectedMessageMap = JanDirectedMessage.union(FebDirectedMessage).union(MarDirectedMessage).union(AprDirectedMessage).union(MayDirectedMessage).union(JunDirectedMessage).union(JulDirectedMessage).union(AugDirectedMessage).union(SepDirectedMessage).groupByKey().map(x => {
        val SnapShot = new Array[(Int, Double, Int, Int, Double)](9)
        x._2.foreach(y => SnapShot(y._6 - 1) = (y._1, y._2, y._3, y._4, y._5))
        for (i <- 0 until 9) {
          if (SnapShot(i) == null)
            SnapShot(i) = (0, 0.0, 0, 0, 0.0)
        }
        (x._1, SnapShot)
      })
      val DirectedMap = DirectedCallMap.map(x => (x._1._1, (x._1._2, x._2))).join(DirectedMessageMap).map(x => {
        ((x._1, x._2._1._1), (x._2._1._2, x._2._2))
      })
      /**DirectedMap.map(x => {
        var out = x._1._1 + "," + x._1._2
        for (i <- 0 until 9) {
          out = out + "," + x._2._1(i)._1 + "," + x._2._1(i)._2 + "," + x._2._2(i)._1 + "," + x._2._2(i)._2 + "," + x._2._2(i)._3 + "," + x._2._2(i)._4 + "," + x._2._2(i)._5
        }
        out
      }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/DirectedMap")**/

      //Undirected
    {
      val JanUnDirectedMap = JanReCallNeighbour.map(x => (x._1, (x._2._1, x._2._2, 1)))
      val FebUnDirectedMap = FebReCallNeighbour.map(x => (x._1, (x._2._1, x._2._2, 2)))
      val MarUnDirectedMap = MarReCallNeighbour.map(x => (x._1, (x._2._1, x._2._2, 3)))
      val AprUnDirectedMap = AprReCallNeighbour.map(x => (x._1, (x._2._1, x._2._2, 4)))
      val MayUnDirectedMap = MayReCallNeighbour.map(x => (x._1, (x._2._1, x._2._2, 5)))
      val JunUnDirectedMap = JunReCallNeighbour.map(x => (x._1, (x._2._1, x._2._2, 6)))
      val JulUnDirectedMap = JulReCallNeighbour.map(x => (x._1, (x._2._1, x._2._2, 7)))
      val AugUnDirectedMap = AugReCallNeighbour.map(x => (x._1, (x._2._1, x._2._2, 8)))
      val SepUnDirectedMap = SepReCallNeighbour.map(x => (x._1, (x._2._1, x._2._2, 9)))

      val JanUnDirectedMessage = JanUnDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 1)))

      val FebUnDirectedMessage = FebUnDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 2)))

      val MarUnDirectedMessage = MarUnDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 3)))

      val AprUnDirectedMessage = AprUnDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 4)))

      val MayUnDirectedMessage = MayUnDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 5)))

      val JunUnDirectedMessage = JunUnDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 6)))

      val JulUnDirectedMessage = JulUnDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 7)))

      val AugUnDirectedMessage = AugUnDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 8)))

      val SepUnDirectedMessage = SepUnDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 9)))

      val UnDirectedCallMap = JanUnDirectedMap.union(FebUnDirectedMap).union(MarUnDirectedMap).union(AprUnDirectedMap).union(MayUnDirectedMap).union(JunUnDirectedMap).union(JulUnDirectedMap).union(AugUnDirectedMap).union(SepUnDirectedMap).groupByKey().map(x => {
        val SnapShot = new Array[(Int, Double)](9)
        x._2.foreach(y => SnapShot(y._3 - 1) = (y._1, y._2))
        for (i <- 0 until 9) {
          if (SnapShot(i) == null)
            SnapShot(i) = (0, 0.0)
        }
        (x._1, SnapShot)
      })
      val UnDirectedMessageMap = JanUnDirectedMessage.union(FebUnDirectedMessage).union(MarUnDirectedMessage).union(AprUnDirectedMessage).union(MayUnDirectedMessage).union(JunUnDirectedMessage).union(JulUnDirectedMessage).union(AugUnDirectedMessage).union(SepUnDirectedMessage).groupByKey().map(x => {
        val SnapShot = new Array[(Int, Double, Int, Int, Double)](9)
        x._2.foreach(y => SnapShot(y._6 - 1) = (y._1, y._2, y._3, y._4, y._5))
        for (i <- 0 until 9) {
          if (SnapShot(i) == null)
            SnapShot(i) = (0, 0.0, 0, 0, 0.0)
        }
        (x._1, SnapShot)
      })
      val UnDirectedMap = UnDirectedCallMap.map(x => (x._1._1, (x._1._2, x._2))).join(UnDirectedMessageMap).map(x => {
        ((x._1, x._2._1._1), (x._2._1._2, x._2._2))
      })
      /**UnDirectedMap.map(x => {
        var out = x._1._1 + "," + x._1._2
        for (i <- 0 until 9) {
          out = out + "," + x._2._1(i)._1 + "," + x._2._1(i)._2 + "," + x._2._2(i)._1 + "," + x._2._2(i)._2 + "," + x._2._2(i)._3 + "," + x._2._2(i)._4 + "," + x._2._2(i)._5
        }
        out
      }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/UnDirectedMap")**/
    }
      //BiDirected
    /**
    {
      val JanBiDirectedMap = JanBidirectionNeighbour.map(x => (x._1, (x._2._1, x._2._2, 1)))
      val FebBiDirectedMap = FebBidirectionNeighbour.map(x => (x._1, (x._2._1, x._2._2, 2)))
      val MarBiDirectedMap = MarBidirectionNeighbour.map(x => (x._1, (x._2._1, x._2._2, 3)))
      val AprBiDirectedMap = AprBidirectionNeighbour.map(x => (x._1, (x._2._1, x._2._2, 4)))
      val MayBiDirectedMap = MayBidirectionNeighbour.map(x => (x._1, (x._2._1, x._2._2, 5)))
      val JunBiDirectedMap = JunBidirectionNeighbour.map(x => (x._1, (x._2._1, x._2._2, 6)))
      val JulBiDirectedMap = JulBidirectionNeighbour.map(x => (x._1, (x._2._1, x._2._2, 7)))
      val AugBiDirectedMap = AugBidirectionNeighbour.map(x => (x._1, (x._2._1, x._2._2, 8)))
      val SepBiDirectedMap = SepBidirectionNeighbour.map(x => (x._1, (x._2._1, x._2._2, 9)))

      val JanBiDirectedMessage = JanBiDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1._1
        val maxT = x._2._1._2
        val neighbourNum = 1
        val sumF = x._2._1._1
        val sumT = x._2._1._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 1)))

      val FebBiDirectedMessage = FebBiDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1._1
        val maxT = x._2._1._2
        val neighbourNum = 1
        val sumF = x._2._1._1
        val sumT = x._2._1._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 2)))

      val MarBiDirectedMessage = MarBiDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1._1
        val maxT = x._2._1._2
        val neighbourNum = 1
        val sumF = x._2._1._1
        val sumT = x._2._1._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 3)))

      val AprBiDirectedMessage = AprBiDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1._1
        val maxT = x._2._1._2
        val neighbourNum = 1
        val sumF = x._2._1._1
        val sumT = x._2._1._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 4)))

      val MayBiDirectedMessage = MayBiDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1._1
        val maxT = x._2._1._2
        val neighbourNum = 1
        val sumF = x._2._1._1
        val sumT = x._2._1._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 5)))

      val JunBiDirectedMessage = JunBiDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1._1
        val maxT = x._2._1._2
        val neighbourNum = 1
        val sumF = x._2._1._1
        val sumT = x._2._1._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 6)))

      val JulBiDirectedMessage = JulBiDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1._1
        val maxT = x._2._1._2
        val neighbourNum = 1
        val sumF = x._2._1._1
        val sumT = x._2._1._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 7)))

      val AugBiDirectedMessage = AugBiDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1._1
        val maxT = x._2._1._2
        val neighbourNum = 1
        val sumF = x._2._1._1
        val sumT = x._2._1._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 8)))

      val SepBiDirectedMessage = SepBiDirectedMap.map(x => {
        val user = x._1._1
        val maxF = x._2._1._1
        val maxT = x._2._1._2
        val neighbourNum = 1
        val sumF = x._2._1._1
        val sumT = x._2._1._2
        (user, (maxF, maxT, neighbourNum, sumF, sumT))
      }).reduceByKey((x, y) => {
        val maxF = max(x._1, y._1)
        val maxT = max(x._2, y._2)
        val neighbourNum = x._3 + y._3
        val sumF = x._4 + y._4
        val sumT = x._5 + y._5
        (maxF, maxT, neighbourNum, sumF, sumT)
      }).map(x => (x._1, (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, 9)))

      val BiDirectedCallMap = JanBiDirectedMap.union(FebBiDirectedMap).union(MarBiDirectedMap).union(AprBiDirectedMap).union(MayBiDirectedMap).union(JunBiDirectedMap).union(JulBiDirectedMap).union(AugBiDirectedMap).union(SepBiDirectedMap).groupByKey().map(x => {
        val SnapShot = new Array[((Int, Double,Double,Double),(Int, Double,Double,Double))](9)
        x._2.foreach(y => SnapShot(y._3 - 1) = (y._1, y._2))
        for (i <- 0 until 9) {
          if (SnapShot(i) == null)
            SnapShot(i) = ((0, 0.0,0.0,0.0),(0, 0.0,0.0,0.0))
        }
        (x._1, SnapShot)
      })
      val BiDirectedMessageMap = JanBiDirectedMessage.union(FebBiDirectedMessage).union(MarBiDirectedMessage).union(AprBiDirectedMessage).union(MayBiDirectedMessage).union(JunBiDirectedMessage).union(JulBiDirectedMessage).union(AugBiDirectedMessage).union(SepBiDirectedMessage).groupByKey().map(x => {
        val SnapShot = new Array[(Int, Double, Int, Int, Double)](9)
        x._2.foreach(y => SnapShot(y._6 - 1) = (y._1, y._2, y._3, y._4, y._5))
        for (i <- 0 until 9) {
          if (SnapShot(i) == null)
            SnapShot(i) = (0, 0.0, 0, 0, 0.0)
        }
        (x._1, SnapShot)
      })
      val BiDirectedMap = BiDirectedCallMap.map(x => (x._1._1, (x._1._2, x._2))).join(BiDirectedMessageMap).map(x => {
        ((x._1, x._2._1._1), (x._2._1._2, x._2._2))
      })
      /**BiDirectedMap.map(x => {
        var out = x._1._1 + "," + x._1._2
        for (i <- 0 until 9) {
          out = out + "," + x._2._1(i)._1 + "," + x._2._1(i)._2 + "," + x._2._2(i)._1 + "," + x._2._2(i)._2 + "," + x._2._2(i)._3 + "," + x._2._2(i)._4 + "," + x._2._2(i)._5
        }
        out
      }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/BiDirectedMap")**/
    }
      **/
    /**
     * TF-IDF Similarity
     */
    val JanServTotalInterestFrequenceIDF = JanServInterestFile.map(x=>{
      val slice = x.split(",")
      val inter = slice(ServInterestBuild.InterestPosition).replaceAll("\"","")
      if(inter.substring(0,1).equals("0")){
        val interest = inter.substring(1,inter.length)
        ((slice(ServInterestBuild.ServInterestIDPosition),interest),slice(ServInterestBuild.InterestFrequencdPosition).toInt)
      }else {
        val interest = inter.substring(0, inter.length)
        ((slice(ServInterestBuild.ServInterestIDPosition),interest),slice(ServInterestBuild.InterestFrequencdPosition).toInt)
      }
    }).reduceByKey(_+_).map(x=>{
      (x._1._2,(x._1._1,x._2))
    }).join(JanServInterestIDF).map(x=>{
      (x._2._1._1,(x._1,x._2._2._2,x._2._1._2,x._2._2._1))
    }).groupByKey().map(x=>{
      val interestVector = new Array[(Double,Double)](643)
      x._2.foreach(y=>interestVector(y._2)=(y._3.toDouble,y._4))
      for(i<-0 until 643){
        if(interestVector(i) == null)
          interestVector(i) = (0.0,0.0)
      }
      (x._1,interestVector)
    })

    val JanTotalMergedInterestFrequencyIDFCDRMap = JanCallingNeighbour.map(x=>(x._1._1,(x._1._2,x._2))).join(JanServTotalInterestFrequenceIDF).map(x=>(x._2._1._1,(x._1,x._2._1._2,x._2._2))).join(JanServTotalInterestFrequenceIDF).map(x=>((x._2._1._1,x._1),(x._2._1._2._1,x._2._1._2._2,x._2._1._3,x._2._2)))
    val JanTotalMergedInterestFrequencyIDFCDRHomo = JanTotalMergedInterestFrequencyIDFCDRMap.map(x=>{
      var sumA = 0.0
      var sumB = 0.0
      for(i<-0 until 643){
        sumA = sumA + x._2._3(i)._1
        sumB = sumB + x._2._4(i)._1
      }
      val A = new Array[Double](643)
      val B = new Array[Double](643)
      for(i<-0 until 643){
        A(i) = x._2._3(i)._1/sumA*x._2._3(i)._2
        B(i) = x._2._4(i)._1/sumB*x._2._4(i)._2
      }
      val similarity = Similarity.AdjustedCosineSimilarity(A,B)
      (x._1,(x._2._1,x._2._2,similarity))
    })
    //val JanIdfInterestSimilarityCloseness = JanTotalMergedInterestFrequencyIDFCDRHomo.join(JanTotalCDRCloseness)
    //JanIdfInterestSimilarityCloseness.map(x=>x._1._1+","+x._1._2+","+x._2._1._1+","+x._2._1._2+","+x._2._1._3+","+x._2._2._1+","+x._2._2._2+","+x._2._2._3+","+x._2._2._4+","+x._2._2._5+","+x._2._2._6+","+x._2._2._7).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JanIdfInterestSimilarityCloseness")
  }
}