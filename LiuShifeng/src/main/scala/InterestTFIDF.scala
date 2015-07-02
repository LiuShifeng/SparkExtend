import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by LiuShifeng on 2015/6/18.
 */
object InterestTFIDF{
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("LiuShifeng_InterestIDF").set("spark.akka.heartbeat.pauses", "1200").set("spark.akka.failure-detector.threshold", "600").set("spark.akka.heartbeat.interval", "2000")
    val sc = new SparkContext(sparkConf)

    val CodePosition = 0 //InterestCode
    val NamePosition = 1 //InterestNamr
    val ParentCodePostion = 2 //ParentNode
    val LevelPosition = 3 //Layer
    val VectorPosition = 4 //VectorId
    val userInterestCodeFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-Code-NanNing.csv"
    val userInterestCodeFile = sc.textFile(userInterestCodeFilePath)
    val userInterestCodeMap = userInterestCodeFile.map(x => {
      val slice = x.split(",")
      (slice(CodePosition), slice(VectorPosition))
    })
    /**
     * 20150701 Subclass Number
     */
    val userInterestSubclassNumber = userInterestCodeMap.map(x=>{
        val code = x._1
        val length = (code.length+1)/2
        val result = new Array[(String,Double)](length)
        for(i<-0 until length){
          result(i) = (code.substring(0,code.length-i*2),1)
        }
        result
    }).flatMap(x=>x).reduceByKey(_+_)
    val userInterestSubclassCodeMap = userInterestCodeMap.join(userInterestSubclassNumber).map(x=>(1,x._2)).groupByKey().map(x=>{
      val result = new Array[Double](643)
      for(i<-x._2){
        result(i._1.toInt) = i._2
      }
      result
    })
//    userInterestSubclassCodeMap.map(_.mkString(",")).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/InterestFilter/userInterestSubclassCodeMap")

    val ServInterest = new ServInterestBuild(userInterestCodeMap)

    val interestFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-2014*.csv"
    val interestFile = sc.textFile(interestFilePath)

    val mergedInterest = ServInterest.MergedInterestFrequence(interestFile)
//    mergedInterest.checkpoint()

    val Jan2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201401.csv"
    val Feb2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201402.csv"
    val Mar2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201403.csv"
    val Apr2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201404.csv"
    val May2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201405.csv"
    val Jun2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201406.csv"
    val Jul2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201407.csv"
    val Aug2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201408.csv"
    val Sep2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201409.csv"
    val Oct2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201410.csv"
    val Nov2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201411.csv"
    val Dec2014CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201412.csv"
    val Jan2015CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201501.csv"
    val Feb2015CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201502.csv"
    val Mar2015CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201503.csv"
    val Apr2015CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201504.csv"


    val Jan2014CDRFile = sc.textFile(Jan2014CDRFilePath)
    val Feb2014CDRFile = sc.textFile(Feb2014CDRFilePath)
    val Mar2014CDRFile = sc.textFile(Mar2014CDRFilePath)
    val Apr2014CDRFile = sc.textFile(Apr2014CDRFilePath)
    val May2014CDRFile = sc.textFile(May2014CDRFilePath)
    val Jun2014CDRFile = sc.textFile(Jun2014CDRFilePath)
    val Jul2014CDRFile = sc.textFile(Jul2014CDRFilePath)
    val Aug2014CDRFile = sc.textFile(Aug2014CDRFilePath)
    val Sep2014CDRFile = sc.textFile(Sep2014CDRFilePath)
    val Oct2014CDRFile = sc.textFile(Oct2014CDRFilePath)
    val Nov2014CDRFile = sc.textFile(Nov2014CDRFilePath)
    val Dec2014CDRFile = sc.textFile(Dec2014CDRFilePath)
    val Jan2015CDRFile = sc.textFile(Jan2015CDRFilePath)
    val Feb2015CDRFile = sc.textFile(Feb2015CDRFilePath)
    val Mar2015CDRFile = sc.textFile(Mar2015CDRFilePath)
    val Apr2015CDRFile = sc.textFile(Apr2015CDRFilePath)

    val CDRFile = Jan2014CDRFile
      .union(Feb2014CDRFile)
      .union(Mar2014CDRFile)
      .union(Apr2014CDRFile)
      .union(May2014CDRFile)
      .union(Jun2014CDRFile)
      .union(Jul2014CDRFile)
      .union(Aug2014CDRFile)
      .union(Sep2014CDRFile)
      .union(Oct2014CDRFile)
      .union(Nov2014CDRFile)
      .union(Dec2014CDRFile)
      .union(Jan2015CDRFile)
      .union(Feb2015CDRFile)
      .union(Mar2015CDRFile)
      .union(Apr2015CDRFile)
    val CDRMap = CallMapBuild.BidirectedRelationMap(CDRFile)
//    CDRMap.checkpoint()
    val userList = CDRMap.map(x=>(x._1._1,1)).reduceByKey(_+_)

    val userInterestList = mergedInterest.join(userList).map(x=>(x._1,x._2._1)).map(x=>{
      val id = x._1
      val userInterest = new Array[Double](x._2.length)
      val l = x._2.length
      for(i<- 0 until l){
        userInterest(i) = if(i!=23) x._2(i) else 0
      }
      (id,userInterest)
    }).filter(_._2.sum>0)
//    userInterestList.checkpoint()

    val interestIDF = idf(userInterestList)
    println(interestIDF.mkString(","))

    val userInterestTFIDf = userInterestList.map(x=>{
      val totalVisitNumber = x._2.sum
      val l = x._2.length
      val userTFIDF = new Array[Double](l)
      for(i<-0 until l){
        userTFIDF(i) = x._2(i)/totalVisitNumber*interestIDF(i)
      }
      (x._1,(userTFIDF,x._2))
    })
//    userInterestTFIDf.map(x=>x._1+","+x._2.mkString(",")).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/InterestFilter/userInterest_TFIDF")
    val InterestUserList = userInterestList.map(x=>x._1)

    val InterestUserListSampleA = InterestUserList.sample(true,0.01,0).repartition(10)
    val InterestUserListSampleB = InterestUserList.sample(true,0.01,0).repartition(10)
    println("InterestUserListSampleA  "+InterestUserListSampleA.count)
    println("InterestUserListSampleA first  "+InterestUserListSampleA.first())
    println("InterestUserListSampleB  "+InterestUserListSampleB.count)
    println("InterestUserListSampleB first  "+InterestUserListSampleB.first())

    val allPair = InterestUserListSampleB.cartesian(InterestUserListSampleA)
    val positivePair = CDRMap.map(x=>x._1)
    println("positivePair "+positivePair.count)
    val negativePair = allPair.subtract(positivePair)
    println("negativePair "+negativePair.count)

    val positiveInterestPair = positivePair.join(userInterestTFIDf).map(x=>(x._2._1,(x._1,x._2._2)))
      .join(userInterestTFIDf).map(x=>((x._2._1._1,x._1),(x._2._1._2,x._2._2)))
    val negativeInterestPair = negativePair.join(userInterestTFIDf).map(x=>(x._2._1,(x._1,x._2._2)))
      .join(userInterestTFIDf).map(x=>((x._2._1._1,x._1),(x._2._1._2,x._2._2)))

//    positiveInterestPair.map(x=>{
//      val userATFIDF_subclass = new Array[Double](643)
//      val userBTFIDF_subclass = new Array[Double](643)
//      for(i<-0 until 643){
//        userATFIDF_subclass(i) = x._2._1._1(i) / attenuationVector.nAttennuation(i)
//        userBTFIDF_subclass(i) = x._2._2._1(i) / attenuationVector.nAttennuation(i)
//      }
//      (x._1,(userATFIDF_subclass,userBTFIDF_subclass))
//    }).map(x=>x._1._1+","+x._1._2+","+x._2._1.mkString(",")+","+x._2._2.mkString(",")).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/InterestFilter/positiveInterestPair")
//    negativeInterestPair.map(x=>{
//      val userATFIDF_subclass = new Array[Double](643)
//      val userBTFIDF_subclass = new Array[Double](643)
//      for(i<-0 until 643){
//        userATFIDF_subclass(i) = x._2._1._1(i) / attenuationVector.nAttennuation(i)
//        userBTFIDF_subclass(i) = x._2._2._1(i) / attenuationVector.nAttennuation(i)
//      }
//      (x._1,(userATFIDF_subclass,userBTFIDF_subclass))
//    }).map(x=>x._1._1+","+x._1._2+","+x._2._1.mkString(",")+","+x._2._2.mkString(",")).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/InterestFilter/negativeInterestPair")


//    val positiveInterestHierachySim = positiveInterestPair.map(x=>(x._1,Similarity.hierachySim(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2)))
//    val negativeInterestHierachySim = negativeInterestPair.map(x=>(x._1,Similarity.hierachySim(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2)))

    val positiveInterestHierachySim = positiveInterestPair.map(x=>{
      val userATFIDF_subclass = new Array[Double](643)
      val userBTFIDF_subclass = new Array[Double](643)
      for(i<-0 until 643){
        userATFIDF_subclass(i) = x._2._1._1(i) / attenuationVector.nAttennuation(i)
        userBTFIDF_subclass(i) = x._2._2._1(i) / attenuationVector.nAttennuation(i)
      }
      (x._1,(Similarity.hierachySim(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2),Similarity.AdjustedCosineSimilarity(userATFIDF_subclass,userBTFIDF_subclass),Similarity.CosineCoefficientSimilarity(userATFIDF_subclass,userBTFIDF_subclass),Similarity.PearsonCorrelationCoefficient(userATFIDF_subclass,userBTFIDF_subclass),Similarity.TanimotoCoefficent(userATFIDF_subclass,userBTFIDF_subclass),Similarity.hierachySim_AdjustedCosine(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2),Similarity.hierachySim_Cosine(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2),Similarity.hierachySim_Pearson(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2),Similarity.hierachySim_Tanimoto(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2)))
    })
    positiveInterestHierachySim.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7+","+x._2._8+","+x._2._9).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/InterestFilter/positiveInterestHierachySim")
    val negativeInterestHierachySim = negativeInterestPair.map(x=>{
      val userATFIDF_subclass = new Array[Double](643)
      val userBTFIDF_subclass = new Array[Double](643)
      for(i<-0 until 643){
        userATFIDF_subclass(i) = x._2._1._1(i) / attenuationVector.nAttennuation(i)
        userBTFIDF_subclass(i) = x._2._2._1(i) / attenuationVector.nAttennuation(i)
      }
      (x._1,(Similarity.hierachySim(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2),Similarity.AdjustedCosineSimilarity(userATFIDF_subclass,userBTFIDF_subclass),Similarity.CosineCoefficientSimilarity(userATFIDF_subclass,userBTFIDF_subclass),Similarity.PearsonCorrelationCoefficient(userATFIDF_subclass,userBTFIDF_subclass),Similarity.TanimotoCoefficent(userATFIDF_subclass,userBTFIDF_subclass),Similarity.hierachySim_AdjustedCosine(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2),Similarity.hierachySim_Cosine(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2),Similarity.hierachySim_Pearson(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2),Similarity.hierachySim_Tanimoto(x._2._1._1,x._2._2._1,x._2._1._2,x._2._2._2)))
    })
    negativeInterestHierachySim.map(x=>x._1._1+","+x._1._2+","+x._2._1+","+x._2._2+","+x._2._3+","+x._2._4+","+x._2._5+","+x._2._6+","+x._2._7+","+x._2._8+","+x._2._9).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/InterestFilter/negativeInterestHierachySim")

    val positiveInterestHierachySimMean = positiveInterestHierachySim.map(x=>("positiveMean",(x._2,1))).reduceByKey((x,y)=>{
      ((x._1._1+y._1._1,x._1._2+y._1._2,x._1._3+y._1._3,x._1._4+y._1._4,x._1._5+y._1._5,x._1._6+y._1._6,x._1._7+y._1._7,x._1._8+y._1._8,x._1._9+y._1._9),x._2+y._2)
    }).map(x=>(x._2._1._1/x._2._2,x._2._1._2/x._2._2,x._2._1._3/x._2._2,x._2._1._4/x._2._2,x._2._1._5/x._2._2,x._2._1._6/x._2._2,x._2._1._7/x._2._2,x._2._1._8/x._2._2,x._2._1._9/x._2._2)).first()
    println("positiveInterestHierachySimMean  "+positiveInterestHierachySimMean)

    val negativeInterestHierachySimMean = negativeInterestHierachySim.map(x=>("negativeMean",(x._2,1))).reduceByKey((x,y)=>{
      ((x._1._1+y._1._1,x._1._2+y._1._2,x._1._3+y._1._3,x._1._4+y._1._4,x._1._5+y._1._5,x._1._6+y._1._6,x._1._7+y._1._7,x._1._8+y._1._8,x._1._9+y._1._9),x._2+y._2)
    }).map(x=>(x._2._1._1/x._2._2,x._2._1._2/x._2._2,x._2._1._3/x._2._2,x._2._1._4/x._2._2,x._2._1._5/x._2._2,x._2._1._6/x._2._2,x._2._1._7/x._2._2,x._2._1._8/x._2._2,x._2._1._9/x._2._2)).first()
    println("negativeInterestHierachySimMean  "+negativeInterestHierachySimMean)

    val positiveInterestHierachySimVariance = positiveInterestHierachySim.map(x=>("positiveVariance",((math.pow(x._2._1-positiveInterestHierachySimMean._1,2),math.pow(x._2._2-positiveInterestHierachySimMean._2,2),math.pow(x._2._3-positiveInterestHierachySimMean._3,2),math.pow(x._2._4-positiveInterestHierachySimMean._4,2),math.pow(x._2._5-positiveInterestHierachySimMean._5,2),math.pow(x._2._6-positiveInterestHierachySimMean._6,2),math.pow(x._2._7-positiveInterestHierachySimMean._7,2),math.pow(x._2._8-positiveInterestHierachySimMean._8,2),math.pow(x._2._9-positiveInterestHierachySimMean._9,2)),1))).reduceByKey((x,y)=>{
      ((x._1._1+y._1._1,x._1._2+y._1._2,x._1._3+y._1._3,x._1._4+y._1._4,x._1._5+y._1._5,x._1._6+y._1._6,x._1._7+y._1._7,x._1._8+y._1._8,x._1._9+y._1._9),x._2+y._2)
    }).map(x=>(x._2._1._1/x._2._2,x._2._1._2/x._2._2,x._2._1._3/x._2._2,x._2._1._4/x._2._2,x._2._1._5/x._2._2,x._2._1._6/x._2._2,x._2._1._7/x._2._2,x._2._1._8/x._2._2,x._2._1._9/x._2._2)).first()
    val negativeInterestHierachySimVariance = negativeInterestHierachySim.map(x=>("negativeVariance",((math.pow(x._2._1-positiveInterestHierachySimMean._1,2),math.pow(x._2._2-positiveInterestHierachySimMean._2,2),math.pow(x._2._3-positiveInterestHierachySimMean._3,2),math.pow(x._2._4-positiveInterestHierachySimMean._4,2),math.pow(x._2._5-positiveInterestHierachySimMean._5,2),math.pow(x._2._6-positiveInterestHierachySimMean._6,2),math.pow(x._2._7-positiveInterestHierachySimMean._7,2),math.pow(x._2._8-positiveInterestHierachySimMean._8,2),math.pow(x._2._9-positiveInterestHierachySimMean._9,2)),1))).reduceByKey((x,y)=>{
      ((x._1._1+y._1._1,x._1._2+y._1._2,x._1._3+y._1._3,x._1._4+y._1._4,x._1._5+y._1._5,x._1._6+y._1._6,x._1._7+y._1._7,x._1._8+y._1._8,x._1._9+y._1._9),x._2+y._2)
    }).map(x=>(x._2._1._1/x._2._2,x._2._1._2/x._2._2,x._2._1._3/x._2._2,x._2._1._4/x._2._2,x._2._1._5/x._2._2,x._2._1._6/x._2._2,x._2._1._7/x._2._2,x._2._1._8/x._2._2,x._2._1._9/x._2._2)).first()
    println("positiveInterestHierachySimVariance  "+positiveInterestHierachySimVariance)
    println("negativeInterestHierachySimVariance  "+negativeInterestHierachySimVariance)

    val positiveInterestHierachySimNum = positiveInterestHierachySim.count
    val negativeInterestHierachySimNum = negativeInterestHierachySim.count
    println("positiveInterestHierachySimNum "+positiveInterestHierachySimNum)
    println("negativeInterestHierachySimNum "+negativeInterestHierachySimNum)

    val tTest = (tScore(positiveInterestHierachySimMean._1,negativeInterestHierachySimMean._1,positiveInterestHierachySimVariance._1,negativeInterestHierachySimVariance._1,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,tScore(positiveInterestHierachySimMean._2,negativeInterestHierachySimMean._2,positiveInterestHierachySimVariance._2,negativeInterestHierachySimVariance._2,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,tScore(positiveInterestHierachySimMean._3,negativeInterestHierachySimMean._3,positiveInterestHierachySimVariance._3,negativeInterestHierachySimVariance._3,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,tScore(positiveInterestHierachySimMean._4,negativeInterestHierachySimMean._4,positiveInterestHierachySimVariance._4,negativeInterestHierachySimVariance._4,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,tScore(positiveInterestHierachySimMean._5,negativeInterestHierachySimMean._5,positiveInterestHierachySimVariance._5,negativeInterestHierachySimVariance._5,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,tScore(positiveInterestHierachySimMean._6,negativeInterestHierachySimMean._6,positiveInterestHierachySimVariance._6,negativeInterestHierachySimVariance._6,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,tScore(positiveInterestHierachySimMean._7,negativeInterestHierachySimMean._7,positiveInterestHierachySimVariance._7,negativeInterestHierachySimVariance._7,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,tScore(positiveInterestHierachySimMean._8,negativeInterestHierachySimMean._8,positiveInterestHierachySimVariance._8,negativeInterestHierachySimVariance._8,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,tScore(positiveInterestHierachySimMean._9,negativeInterestHierachySimMean._9,positiveInterestHierachySimVariance._9,negativeInterestHierachySimVariance._9,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      )
    println("tTest "+tTest)

    val df = (degreeOfFreedom(positiveInterestHierachySimVariance._1,negativeInterestHierachySimVariance._1,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,degreeOfFreedom(positiveInterestHierachySimVariance._2,negativeInterestHierachySimVariance._2,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,degreeOfFreedom(positiveInterestHierachySimVariance._3,negativeInterestHierachySimVariance._3,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,degreeOfFreedom(positiveInterestHierachySimVariance._4,negativeInterestHierachySimVariance._4,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,degreeOfFreedom(positiveInterestHierachySimVariance._5,negativeInterestHierachySimVariance._5,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,degreeOfFreedom(positiveInterestHierachySimVariance._6,negativeInterestHierachySimVariance._6,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,degreeOfFreedom(positiveInterestHierachySimVariance._7,negativeInterestHierachySimVariance._7,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,degreeOfFreedom(positiveInterestHierachySimVariance._8,negativeInterestHierachySimVariance._8,positiveInterestHierachySimNum,negativeInterestHierachySimNum)
      ,degreeOfFreedom(positiveInterestHierachySimVariance._9,negativeInterestHierachySimVariance._9,positiveInterestHierachySimNum,negativeInterestHierachySimNum)  )
    println("df "+df)
  }

  def tScore(Xa:Double,Xb:Double,Va:Double,Vb:Double,Na:Long,Nb:Long):Double={
    (Xa-Xb)/(math.sqrt(Va/Na+Vb/Nb))
  }

  def degreeOfFreedom(Va:Double,Vb:Double,Na:Long,Nb:Long):Double={
    math.pow((Va/Na+Va/Nb),2)/(math.pow(Va/Na,2)/(Na-1)+math.pow(Vb/Nb,2)/(Nb-1))
  }

  def idf(info:RDD[(String,Array[Double])]):Array[Double]={
    val userNumber = info.count()
    val idf = info.map(x=>{
      val l = x._2.length
      val userVisit = new Array[Double](l)
      for(i<-0 until l){
        userVisit(i) = if(x._2(i)>0) 1 else 0
      }
      ("user",userVisit)
    }).reduceByKey((x,y)=>{
      val l = x.length
      val userVisit = new Array[Double](l)
      for(i<- 0 until l){
        userVisit(i) = x(i) + y(i)
      }
      userVisit
    }).map(x=>{
      val l = x._2.length
      val result = new Array[Double](l)
      for(i<- 0 until l){
        result(i) = math.log(userNumber/(x._2(i)+1))
      }
      result
    }).take(1)(0)
    idf
  }
}
