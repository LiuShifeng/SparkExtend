import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by LiuShifeng on 2015/1/28.
 */

object ClosenessSimilarity {
  def main(args: Array[String]) {
    if(args.length<5){
      System.out.println("Error!")
      System.exit(1)
    }
    val destPath = args(0)
    val Month = args(1)
    val InterestType = args(2)
    val CDRType = args(3)
    val SimilarityType = args(4)

    val sparkConf = new SparkConf().setAppName("ClosenesswithSimilarity").set("spark.akka.heartbeat.pauses", "1200").set("spark.akka.failure-detector.threshold", "600").set("spark.akka.heartbeat.interval", "2000")
    val sc = new SparkContext(sparkConf)

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

//    val CDR = new CallMapBuild(sc)
    val Interest = new ServInterestBuild(InterestCodeMap)

    val CDRFile = Month match{
      case "Jan" => sc.textFile(CallMapBuild.JanCDRPath)
      case "Feb" => sc.textFile(CallMapBuild.FebCDRPath)
      case "Mar" => sc.textFile(CallMapBuild.MarCDRPath)
      case "Apr" => sc.textFile(CallMapBuild.AprCDRPath)
      case "May" => sc.textFile(CallMapBuild.MayCDRPath)
      case "Jun" => sc.textFile(CallMapBuild.JunCDRPath)
      case "Jul" => sc.textFile(CallMapBuild.JulCDRPath)
      case "Aug" => sc.textFile(CallMapBuild.AugCDRPath)
      case "Sep" => sc.textFile(CallMapBuild.SepCDRPath)
    }

    val InterestFile = Month match{
      case "Jan" => sc.textFile(Interest.JanServInterestPath)
      case "Feb" => sc.textFile(Interest.FebServInterestPath)
      case "Mar" => sc.textFile(Interest.MarServInterestPath)
      case "Apr" => sc.textFile(Interest.AprServInterestPath)
      case "May" => sc.textFile(Interest.JanServInterestPath)
      case "Jun" => sc.textFile(Interest.JunServInterestPath)
      case "Jul" => sc.textFile(Interest.JulServInterestPath)
      case "Aug" => sc.textFile(Interest.AugServInterestPath)
      case "Sep" => sc.textFile(Interest.SepServInterestPath)
    }

    val CDRMap = CDRType match {
      case "Directed" => CallMapBuild.DirectedFrequencyDurationDistributionRelationMap(CDRFile)
      case "UnDirected" => CallMapBuild.UnDirectedFrequencyDurationDistributionRelationMap(CDRFile)
      case "ReversedDirected" => CallMapBuild.ReversedDirectedFrequencyDurationDistributionRelationMap(CDRFile)
      case "BiDirected" => CallMapBuild.BiDirectedFrequencyDurationDistributionRelationMap(CDRFile)
    }
    CDRMap.cache()

    val InterestMatrix = new InterestMatrixBuild(InterestFile,InterestCodeMap)
    val InterestMap = InterestType match{
      case "Merge" => Interest.MergedInterestFrequence(InterestFile)
      case "Statistic" => Interest.StatisticInterestFrequence(InterestFile)
      case "24" => InterestMatrix.get24Dimension()
      case "23" => InterestMatrix.get23Dimension()
      case "643" => InterestMatrix.get643Dimension() //Statistic
      case "642" => InterestMatrix.get642Dimension() //Statistic
    }
    InterestMap.cache()

    val ClosenessMap = CallMapBuild.CDRFrequencyDurationDistributionMessage(CDRMap,0).cache()
    if(SimilarityType.equals("All")){
      val SimilarityMap = Interest.Interest28AllSim(CDRMap.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,x._2._5.toArray))),InterestMap)
      val CS = SimilarityMap.join(ClosenessMap)

      CS.map(x=>{
        val user = x._1._1+","+x._1._2
        var Adjusted = x._2._1._1(0).toString
        val AdjustedLength = x._2._1._1.length
        var Cosine = x._2._1._2(0).toString
        val CosineLength = x._2._1._2.length
        var Pearson = x._2._1._3(0).toString
        val PearsonLength = x._2._1._3.length
        var Jaccard = x._2._1._4(0).toString
        val JaccardLength = x._2._1._4.length
        var Tanimoto = x._2._1._5(0).toString
        val TanimotoLength = x._2._1._5.length
        for(i<-0 until AdjustedLength){
          Adjusted = Adjusted + "," +x._2._1._1(i)
        }
        for(i<-0 until CosineLength){
          Cosine = Cosine + "," +x._2._1._2(i)
        }
        for(i<-0 until PearsonLength){
          Pearson = Pearson + "," +x._2._1._3(i)
        }
        for(i<-0 until JaccardLength){
          Jaccard = Jaccard + "," +x._2._1._4(i)
        }
        for(i<-0 until TanimotoLength){
          Tanimoto = Tanimoto + "," +x._2._1._5(i)
        }
        val Similarity = Adjusted+","+Cosine+","+Pearson+","+Jaccard+","+Tanimoto
        val Closeness = x._2._2._1._1+","+x._2._2._1._2+","+x._2._2._1._3+","+x._2._2._1._4+","+x._2._2._1._5+","+x._2._2._1._6+","+x._2._2._1._7+","+x._2._2._1._8+","+x._2._2._1._9+","+x._2._2._1._10
        val Message = x._2._2._2._1+","+x._2._2._2._2+","+x._2._2._2._3+","+x._2._2._2._4+","+x._2._2._2._5+","+x._2._2._2._6+","+x._2._2._2._7
        val rank = x._2._2._3._1+","+x._2._2._3._2+","+x._2._2._3._3+","+x._2._2._3._4+","+x._2._2._3._5+","+x._2._2._3._6+","+x._2._2._3._7+","+x._2._2._3._8+","+x._2._2._3._9+","+x._2._2._3._10+","+x._2._2._3._11
        user+","+Similarity+","+Closeness+","+rank+","+Message
      }).saveAsTextFile(destPath)
    }else{
      val SimilarityMap = SimilarityType match{
        case "Adjusted" => Interest.Interest28SimFrequencyDurationDistribution(CDRMap.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,x._2._5.toArray))),InterestMap,0)
        case "Cosin" => Interest.Interest28SimFrequencyDurationDistribution(CDRMap.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,x._2._5.toArray))),InterestMap,2)
        case "Pearson" => Interest.Interest28SimFrequencyDurationDistribution(CDRMap.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,x._2._5.toArray))),InterestMap,3)
        case "Jaccard" => Interest.Interest28SimFrequencyDurationDistribution(CDRMap.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,x._2._5.toArray))),InterestMap,4)
        case "Tanimoto" => Interest.Interest28SimFrequencyDurationDistribution(CDRMap.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._4,x._2._5.toArray))),InterestMap,5)
      }
      val CS = SimilarityMap.join(ClosenessMap)

      CS.map(x=>{
        val user = x._1._1+","+x._1._2
        var Similarity = x._2._1._2(0).toString
        val SimiLength = x._2._1._2.length
        for(i<- 1 until SimiLength){
          Similarity = Similarity + "," +x._2._1._2(i)
        }
        val Closeness = x._2._2._1._1+","+x._2._2._1._2+","+x._2._2._1._3+","+x._2._2._1._4+","+x._2._2._1._5+","+x._2._2._1._6+","+x._2._2._1._7+","+x._2._2._1._8+","+x._2._2._1._9+","+x._2._2._1._10
        val Message = x._2._2._2._1+","+x._2._2._2._2+","+x._2._2._2._3+","+x._2._2._2._4+","+x._2._2._2._5+","+x._2._2._2._6+","+x._2._2._2._7
        val rank = x._2._2._3._1+","+x._2._2._3._2+","+x._2._2._3._3+","+x._2._2._3._4+","+x._2._2._3._5+","+x._2._2._3._6+","+x._2._2._3._7+","+x._2._2._3._8+","+x._2._2._3._9+","+x._2._2._3._10+","+x._2._2._3._11
        user+","+Similarity+","+Closeness+","+rank+","+Message
      }).saveAsTextFile(destPath)
    }
/**
    val file = sc.textFile(JanServInterestPath)
    val InterestVector = file.map(x => {
      val slice = x.split(",")
      val inter = slice(2).replaceAll("\"", "")
      if (inter.substring(0, 1).equals("0")) {
        val interest = inter.substring(1, inter.length)
        ((slice(1), interest), slice(3).toInt)
      } else {
        val interest = inter.substring(0, inter.length)
        ((slice(1), interest), slice(3).toInt)
      }
    }).reduceByKey(_ + _).map(x => {
      (x._1._2, (x._1._1, x._2))
    }).join(InterestCodeMap).map(x => {
      (x._2._1._1, (x._1, x._2._2.toInt, x._2._1._2))
    }).groupByKey().map(x => {
      val interestVector = new Array[Double](643)
      x._2.foreach(y => interestVector(y._2) = y._3.toDouble)
      var out = interestVector(0).toString
      for(i<-1 until 643){
        out = out + "," + interestVector(i)
      }
      (x._1, out)
    })
    import org.apache.spark.mllib.clustering.KMeans
    import org.apache.spark.mllib.linalg.Vectors
    val parsedData = InterestVector.map(x=>x._2).map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    val EArray = new Array[Double](12)

    for(i<-1 to 12){
      // Cluster the data into two classes using KMeans
      val numClusters = i*50
      val numIterations = 50
      val clusters = KMeans.train(parsedData, numClusters, numIterations)

      // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = clusters.computeCost(parsedData)

      EArray(i-1) = WSSSE
    }
    EArray.take(15)
    val JanCDRFile = sc.textFile(CallMapBuild.JanCDRPath)
    val JanNeighboutNum = JanCDRFile.map(x => {
      val slice = x.split(",")
      val result = new Array[((String,String),Int)](2)
      result(0) = ((slice(0), slice(5)), 1)
      result(1) = ((slice(5), slice(0)), 1)
      result
    }).flatMap(x=>x).reduceByKey(_+_).map(x=>(x._1._1,1)).reduceByKey(_+_)
    val f = JanNeighboutNum.filter(x=>(!x._1.contains("-"))&&(x._2==1))
  **/
    val item = 1
    val level = 3
    def Entropy(file:RDD[String],p1:Int,p2:Int)= {
      file.map(x=>{
        val s = x.split(",")
        ((s(p1),s(p2)),1.0)
      }).reduceByKey(_+_).map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().map(x=>{
        val l = x._2.size
        val a = new Array[Double](l)
        var i = 0
        x._2.foreach(y=> {if(i<l) a(i) = y._2;i = i+1})
        val sum = a.sum
        var e = 0.0
        for(i<-0 until l){
          val p = a(i)/sum
          e = e - p*Math.log10(p)/Math.log10(2)
        }
        (x._1,e)
      })
    }
}}