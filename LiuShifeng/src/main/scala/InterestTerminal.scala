/**
 * Created by LiuShifeng on 2015/5/19.
 */

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object InterestTermianl{
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("InterestTerminal").set("spark.akka.heartbeat.pauses", "1200").set("spark.akka.failure-detector.threshold", "600").set("spark.akka.heartbeat.interval", "2000")
    val sc = new SparkContext(sparkConf)

    val CodePosition = 0 //InterestCode
    val NamePosition = 1 //InterestName
    val ParentCodePostion = 2 //InterestParentCode
    val LevelPosition = 3 //InterestLevel
    val VectorPosition = 4 //VectorId
    val userInterestCodeFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-Code-NanNing.csv"
    val userInterestCodeFile = sc.textFile(userInterestCodeFilePath)
    val userInterestCodeMap = userInterestCodeFile.map(x => {
      val slice = x.split(",")
      (slice(CodePosition), slice(VectorPosition))
    })

    val JulInterestFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201407.csv"
    val AugInterestFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201408.csv"
    val SepInterestFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201409.csv"
    val OctInterestFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201410.csv"
    val JulInterestFile = sc.textFile(JulInterestFilePath)
    val AugInterestFile = sc.textFile(AugInterestFilePath)
    val SepInterestFile = sc.textFile(SepInterestFilePath)
    val OctInterestFile = sc.textFile(OctInterestFilePath)
    val JulInterest = new InterestMatrixBuild(JulInterestFile,userInterestCodeMap)
    val AugInterest = new InterestMatrixBuild(AugInterestFile,userInterestCodeMap)
    val SepInterest = new InterestMatrixBuild(SepInterestFile,userInterestCodeMap)
    val OctInterest = new InterestMatrixBuild(OctInterestFile,userInterestCodeMap)
    val JulInterestDimension24 = JulInterest.get24Dimension().map(x=>(x._1,(7,x._2)))
    val AugInterestDimension24 = AugInterest.get24Dimension().map(x=>(x._1,(8,x._2)))
    val SepInterestDimension24 = SepInterest.get24Dimension().map(x=>(x._1,(9,x._2)))
    val OctInterestDimension24 = OctInterest.get24Dimension().map(x=>(x._1,(10,x._2)))

    val JulTerminalFilePath = "hdfs://dell01:12306/user/tele/data/nanning/Terminal/Terminal-NanNing-201407.csv"
    val AugTerminalFilePath = "hdfs://dell01:12306/user/tele/data/nanning/Terminal/Terminal-NanNing-201408.csv"
    val SepTerminalFilePath = "hdfs://dell01:12306/user/tele/data/nanning/Terminal/Terminal-NanNing-201409.csv"
    val OctTerminalFilePath = "hdfs://dell01:12306/user/tele/data/nanning/Terminal/Terminal-NanNing-201410.csv"
    val JulTerminalFile = sc.textFile(JulTerminalFilePath)
    val AugTerminalFile = sc.textFile(AugTerminalFilePath)
    val SepTerminalFile = sc.textFile(SepTerminalFilePath)
    val OctTerminalFile = sc.textFile(OctTerminalFilePath)
    val JulTerminal = new TerminalFeature(JulTerminalFile)
    val AugTerminal = new TerminalFeature(AugTerminalFile)
    val SepTerminal = new TerminalFeature(SepTerminalFile)
    val OctTerminal = new TerminalFeature(OctTerminalFile)
    val JulTerminalFeature = JulTerminal.getFeature().map(x=>(x._1,x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7)).groupByKey().map(x=>{var result = "";x._2.foreach(y=>result = result+y+",");(x._1,result.substring(0,result.length-1))})
    val AugTerminalFeature = AugTerminal.getFeature().map(x=>(x._1,x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7)).groupByKey().map(x=>{var result = "";x._2.foreach(y=>result = result+y+",");(x._1,result.substring(0,result.length-1))})
    val SepTerminalFeature = SepTerminal.getFeature().map(x=>(x._1,x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7)).groupByKey().map(x=>{var result = "";x._2.foreach(y=>result = result+y+",");(x._1,result.substring(0,result.length-1))})
    val OctTerminalFeature = OctTerminal.getFeature().map(x=>(x._1,x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7)).groupByKey().map(x=>{var result = "";x._2.foreach(y=>result = result+y+",");(x._1,result.substring(0,result.length-1))})

    val JulAugInterestTerminal = JulInterestDimension24.join(AugInterestDimension24).leftOuterJoin(AugTerminalFeature).map(x=>{
      val id = x._1
      val JulInterest = x._2._1._1
      val AugInterest = x._2._1._2
      val terminal = x._2._2 match {case Some(z)=>z; case None =>"None"}
      val AdjustedCos = Similarity.AdjustedCosineSimilarity(JulInterest._2.map(_.toDouble),AugInterest._2.map(_.toDouble))
      (id,(AdjustedCos,terminal,JulInterest,AugInterest))
    }).leftOuterJoin(SepTerminalFeature).map(x=>{
      val terminal = x._2._2 match {case Some(z)=>z; case None =>"None"}
      (x._1,(x._2._1._1,x._2._1._2,terminal,x._2._1._3,x._2._1._4))
    })

    val JulAugTerminalNC = JulAugInterestTerminal.filter(x=>(!x._2._2.equals("None")) && (!x._2._3.equals("None")))
    val JulAugTerminalFC = JulAugInterestTerminal.filter(x=>(x._2._2.equals("None")) && (x._2._3.equals("None")))
    val JulAugTerminalC = JulAugInterestTerminal.filter(x=>(!x._2._2.equals("None")) || (!x._2._3.equals("None")))

    val JulAugInterestTerminal_AC05 = JulAugInterestTerminal.filter(x=>x._2._1<0.5 && (!x._2._2.equals("None")))
  }
}