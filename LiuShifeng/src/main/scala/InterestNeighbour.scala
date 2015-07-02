import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by LiuShifeng on 2015/6/15.
 */
object InteresetNeighbour{
  def main(args: Array[String]) {
    if(args.length<5){
      System.out.println("Error!")
      System.exit(1)
    }

    val CDRFilePath = args(0)
    val InterestFilePath = args(1)
    val InterestDistributionDestPath = args(2)
    val CDRInterestFilterDestPath = args(3)
    val CDRInterestFilterReleventDestPath = args(4)

    val sparkConf = new SparkConf().setAppName("LiuShifeng_InterestNeighbour").set("spark.akka.heartbeat.pauses", "1200").set("spark.akka.failure-detector.threshold", "600").set("spark.akka.heartbeat.interval", "2000")
    val sc = new SparkContext(sparkConf)

    val CDRMap = CallMapBuild.BiDirectedFrequencyDurationDistributionRelationMap(sc.textFile(CDRFilePath))
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

    val Interest = new InterestMatrixBuild(sc.textFile(InterestFilePath),userInterestCodeMap)
    val Interest23D = Interest.get23Dimension().filter(_._2.sum!=0)

    val InterestDistribution = CDRMap.map(x=>(x._1._1,1)).reduceByKey(_+_).join(Interest23D).map(x=>(x._1,x._2._2)).filter(x=>x._2.sum!=0)

    val InterestNoneArray = new Array[Double](23)
    val CDRInterestMap = CDRMap.map(x=>(x._1._1,(x._1._2,x._2))).join(InterestDistribution).map(x=>(x._2._1._1,(x._1,x._2._2,x._2._1._2))).join(InterestDistribution).map(x=>((x._2._1._1,x._1),(x._2._1._2,x._2._2,x._2._1._3)))
    val CDRInterestReleventMap = CDRMap.map(x=>(x._1._1,(x._1._2,x._2))).leftOuterJoin(InterestDistribution).map(x=>(x._2._1._1,(x._1,x._2._2 match{ case Some(z) => z;case None =>new Array[Double](23)},x._2._1._2))).leftOuterJoin(InterestDistribution).map(x=>((x._2._1._1,x._1),(x._2._1._2,x._2._2 match{ case Some(z) => z;case None =>new Array[Double](23)},x._2._1._3)))

    val InterestConcentrate = CDRInterestReleventMap.map(x=>{
      val caller = x._1._1
      val callee = x._1._2
      val callerInterest = x._2._1
      val calleeInterest = x._2._2
      val callInfo = x._2._3
      val NeighbourInterest = new Array[Double](23)
      for(i<- 0 until 23){
        NeighbourInterest(i) = if(calleeInterest(i)>1) 1 else 0
      }
      (caller,(callerInterest,NeighbourInterest,1))
    }).reduceByKey((x,y)=>{
      val NeighbourInterest = new Array[Double](23)
      for(i<-0 until 23){
        NeighbourInterest(i) = x._2(i) + y._2(i)
      }
      (x._1,NeighbourInterest,x._3+y._3)
    }).map(x=>{
      val NeighbourInterest = new Array[Double](23)
      for(i<-0 until 23){
        NeighbourInterest(i) = x._2._2(i)/x._2._3
      }
      (x._1,(x._2._1,x._2._2,NeighbourInterest))
    })

    val userInterestInference = InterestConcentrate.filter(_._2._1.sum>0)
    val InterestNeighboutDistribution = userInterestInference.map(x=>{
      val out = x._2._1.mkString(",")+","+x._2._3.mkString(",")
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/InterestFilter/Jan/InterestNeighboutDistribution")


  }
}
