import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class ServInterestBuild(val InterestCodeMap:RDD[(String,String)])extends Serializable {
  val InterestCodeFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-Code-NanNing.csv"
  val CodePosition = 0
  val NamePosition = 1
  val ParentCodePostion = 2
  val LevelPosition = 3
  val VectorPosition = 4

  val JanServInterestPath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201401.csv"
  val FebServInterestPath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201402.csv"
  val MarServInterestPath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201403.csv"
  val AprServInterestPath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201404.csv"
  val JunServInterestPath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201406.csv"
  val JulServInterestPath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201407.csv"
  val AugServInterestPath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201408.csv"
  val SepServInterestPath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201409.csv"

  //ServInterest Variables
  val ServInterestIDPosition = 1
  val InterestPosition = 2
  val InterestFrequencdPosition = 3
  val InterestVisitTimePosition = 0

  val InterestLayerOne = 24
  val InterestLayerTwo = 166
  val InterestLayerThree = 475
  val InterestLayerFour = 643

  /**
   /**
    * file build
    */
   val JanServInterestFile = sc.textFile(JanServInterestPath)
   val FebServInterestFile = sc.textFile(FebServInterestPath)
   val MarServInterestFile = sc.textFile(MarServInterestPath)
   val AprServInterestFile = sc.textFile(AprServInterestPath)
   val JunServInterestFile = sc.textFile(JunServInterestPath)
   val JulServInterestFile = sc.textFile(JulServInterestPath)
   val AugServInterestFile = sc.textFile(AugServInterestPath)
   val SepServInterestFile = sc.textFile(SepServInterestPath)

   /**
    * Interest 643 Vector
    */
   val JanStatisticInterestFrequence = StatisticInterestFrequence(JanServInterestFile)
   val FebStatisticInterestFrequence = StatisticInterestFrequence(FebServInterestFile)
   val MarStatisticInterestFrequence = StatisticInterestFrequence(MarServInterestFile)
   val AprStatisticInterestFrequence = StatisticInterestFrequence(AprServInterestFile)
   val JunStatisticInterestFrequence = StatisticInterestFrequence(JunServInterestFile)
   val JulStatisticInterestFrequence = StatisticInterestFrequence(JulServInterestFile)
   val AugStatisticInterestFrequence = StatisticInterestFrequence(AugServInterestFile)
   val SepStatisticInterestFrequence = StatisticInterestFrequence(SepServInterestFile)
   /**
    * INterest Merged Vector
    */
   val JanServTotalMergedInterestFrequence = MergedInterestFrequence(JanServInterestFile)
   val FebServTotalMergedInterestFrequence = MergedInterestFrequence(FebServInterestFile)
   val MarServTotalMergedInterestFrequence = MergedInterestFrequence(MarServInterestFile)
   val AprServTotalMergedInterestFrequence = MergedInterestFrequence(AprServInterestFile)
   val JunServTotalMergedInterestFrequence = MergedInterestFrequence(JunServInterestFile)
   val JulServTotalMergedInterestFrequence = MergedInterestFrequence(JulServInterestFile)
   val AugServTotalMergedInterestFrequence = MergedInterestFrequence(AugServInterestFile)
   val SepServTotalMergedInterestFrequence = MergedInterestFrequence(SepServInterestFile)
**/
  def MergedInterestFrequence(file: RDD[String]) = {
    val Merged = file.map(x => {
      val slice = x.split(",")
      val inter = slice(InterestPosition).replaceAll("\"", "")
      val id = slice(1)
      val f = slice(3).toInt
      if (inter.substring(0, 1).equals("0")) {
        val interest = inter.substring(1, inter.length)
        val l = interest.length
        if (l <= 2) {
          id + "," + interest + "," + f
        } else if (l <= 4) {
          id + "," + interest + "," + f + "#" + id + "," + interest.substring(0, l - 2) + "," + f
        } else if (l <= 6) {
          id + "," + interest + "," + f + "#" + id + "," + interest.substring(0, l - 2) + "," + f + "#" + id + "," + interest.substring(0, l - 4) + "," + f
        } else if (l <= 8) {
          id + "," + interest + "," + f + "#" + id + "," + interest.substring(0, l - 2) + "," + f + "#" + id + "," + interest.substring(0, l - 4) + "," + f + "#" + id + "," + interest.substring(0, l - 6) + "," + f
        } else {
          x
        }
      } else {
        val interest = inter.substring(0, inter.length)
        val l = interest.length
        if (l <= 2) {
          id + "," + interest + "," + f
        } else if (l <= 4) {
          id + "," + interest + "," + f + "#" + id + "," + interest.substring(0, l - 2) + "," + f
        } else if (l <= 6) {
          id + "," + interest + "," + f + "#" + id + "," + interest.substring(0, l - 2) + "," + f + "#" + id + "," + interest.substring(0, l - 4) + "," + f
        } else if (l <= 8) {
          id + "," + interest + "," + f + "#" + id + "," + interest.substring(0, l - 2) + "," + f + "#" + id + "," + interest.substring(0, l - 4) + "," + f + "#" + id + "," + interest.substring(0, l - 6) + "," + f
        } else {
          x
        }
      }
    }).flatMap(_.split("#")).map(x => {
      val slice = x.split(",")
      ((slice(0), slice(1)), slice(2).toInt)
    }).reduceByKey(_ + _).map(x => {
      (x._1._2, (x._1._1, x._2))
    }).join(InterestCodeMap).map(x => {
      (x._2._1._1, (x._1, x._2._2.toInt, x._2._1._2))
    }).groupByKey().map(x => {
      val interestVector = new Array[Double](643)
      x._2.foreach(y => interestVector(y._2) = y._3.toDouble)
      (x._1, interestVector)
    })
    Merged
  }
  def StatisticInterestFrequence(file: RDD[String]) = {
    val statistic = file.map(x => {
      val slice = x.split(",")
      val inter = slice(InterestPosition).replaceAll("\"", "")
      if (inter.substring(0, 1).equals("0")) {
        val interest = inter.substring(1, inter.length)
        ((slice(ServInterestIDPosition), interest), slice(InterestFrequencdPosition).toInt)
      } else {
        val interest = inter.substring(0, inter.length)
        ((slice(ServInterestIDPosition), interest), slice(InterestFrequencdPosition).toInt)
      }
    }).reduceByKey(_ + _).map(x => {
      (x._1._2, (x._1._1, x._2))
    }).join(InterestCodeMap).map(x => {
      (x._2._1._1, (x._1, x._2._2.toInt, x._2._1._2))
    }).groupByKey().map(x => {
      val interestVector = new Array[Double](643)
      x._2.foreach(y => interestVector(y._2) = y._3.toDouble)
      (x._1, interestVector)
    })
    statistic
  }
  /**
   * NeighbourPair Interest Similarity with frenquency which is directly calculated from the file(without merge)
   */
  def InterestHomo(RelationMap:RDD[((String, String), (Int, Double,Double,Double))],InterstStatistic:RDD[(String,Array[Double])],Type:Int)={
    val TotalInterestFrequencyCDRMap = RelationMap.map(x=>(x._1._1,(x._1._2,x._2))).join(InterstStatistic).map(x=>(x._2._1._1,(x._1,x._2._1._2,x._2._2))).join(InterstStatistic).map(x=>((x._2._1._1,x._1),(x._2._1._2._1,x._2._1._2._2,x._2._1._3,x._2._2)))
    val TotalInterestFrequencyCDRHomo = TotalInterestFrequencyCDRMap.map(x=>{
      val P1L1 = new Array[Double](23)
      val P2L1 = new Array[Double](23)
      val P1L2 = new Array[Double](166-24)
      val P2L2 = new Array[Double](166-24)
      val P1L3 = new Array[Double](475-166)
      val P2L3 = new Array[Double](475-166)
      val P1L4 = new Array[Double](643-475)
      val P2L4 = new Array[Double](643-475)
      for(i<-0 until 643 if i != 23){
        if(i<24){
          P1L1(i) = x._2._3(i)
          P2L1(i) = x._2._4(i)
        }else if(i<166){
          P1L2(i-24) = x._2._3(i)
          P2L2(i-24) = x._2._4(i)
        }else if(i<475){
          P1L3(i-166) = x._2._3(i)
          P2L3(i-166) = x._2._4(i)
        }else if(i<643){
          P1L4(i-475) = x._2._3(i)
          P2L4(i-475) = x._2._4(i)
        }
      }
      var similarity1 = Similarity.AdjustedCosineSimilarity(P1L1,P2L1)
      var similarity2 = Similarity.AdjustedCosineSimilarity(P1L2,P2L2)
      var similarity3 = Similarity.AdjustedCosineSimilarity(P1L3,P2L3)
      var similarity4 = Similarity.AdjustedCosineSimilarity(P1L4,P2L4)
      if(Type == 1){
        var similarity1 = Similarity.AdjustedCosineSimilarityWithoutZeros(P1L1,P2L1)
        var similarity2 = Similarity.AdjustedCosineSimilarityWithoutZeros(P1L2,P2L2)
        var similarity3 = Similarity.AdjustedCosineSimilarityWithoutZeros(P1L3,P2L3)
        var similarity4 = Similarity.AdjustedCosineSimilarityWithoutZeros(P1L4,P2L4)
      }else{
        var similarity1 = Similarity.AdjustedCosineSimilarity(P1L1,P2L1)
        var similarity2 = Similarity.AdjustedCosineSimilarity(P1L2,P2L2)
        var similarity3 = Similarity.AdjustedCosineSimilarity(P1L3,P2L3)
        var similarity4 = Similarity.AdjustedCosineSimilarity(P1L4,P2L4)
      }
      (x._1,(similarity1,similarity2,similarity3,similarity4))
    })
    TotalInterestFrequencyCDRHomo
  }
  def Interest28Sim(RelationMap:RDD[((String, String), (Int, Double,Double,Double))],InterstStatistic:RDD[(String,Array[Double])],Type:Int)={
    val TotalInterestFrequencyCDRMap = RelationMap.map(x=>(x._1._1,(x._1._2,x._2))).join(InterstStatistic).map(x=>(x._2._1._1,(x._1,x._2._1._2,x._2._2))).join(InterstStatistic).map(x=>((x._2._1._1,x._1),(x._2._1._2._1,x._2._1._2._2,x._2._1._3,x._2._2)))
    val TotalInterestFrequencyCDRHomo = TotalInterestFrequencyCDRMap.map(x=>{
      if(Type==1){
        (x._1, Similarity.AdjustedCosineSimilarity4InterestKZ(x._2._3, x._2._4))
      }else {
        (x._1, Similarity.AdjustedCosineSimilarity4Interest(x._2._3, x._2._4))
      }
    })
    TotalInterestFrequencyCDRHomo
  }

  /**
   *
   * @param RelationMap
   * @param InterstStatistic
   * @param Type
   * @return
   */
  def Interest28SimwithCallmessage(RelationMap:RDD[((String, String), (Int, Double,Double,Double,Array[Long]))],InterstStatistic:RDD[(String,Array[Double])],Type:Int)={
    val TotalInterestFrequencyCDRMap = RelationMap.map(x=>(x._1._1,(x._1._2,x._2))).join(InterstStatistic).map(x=>(x._2._1._1,(x._1,x._2._1._2,x._2._2))).join(InterstStatistic).map(x=>((x._2._1._1,x._1),(x._2._1._2,x._2._1._3,x._2._2)))
    val TotalInterestFrequencyCDRHomo = TotalInterestFrequencyCDRMap.map(x=>{
      if(Type==1){
        (x._1, (x._2._1,Similarity.AdjustedCosineSimilarity4InterestKZ(x._2._2, x._2._3)))
      }else {
        (x._1, (x._2._1,Similarity.AdjustedCosineSimilarity4Interest(x._2._2, x._2._3)))
      }
    })
    TotalInterestFrequencyCDRHomo
  }

  /**
   *
   * @param RelationMap
   * @param InterstStatistic
   * @param Type
   * @return
   */
  def Interest28SimFrequencyDurationDistribution(RelationMap:RDD[((String, String), (Array[Double], Array[Double],Array[Double],Array[Double],Array[Long]))],InterstStatistic:RDD[(String,Array[Double])],Type:Int)={
    val TotalInterestFrequencyCDRMap = RelationMap.map(x=>(x._1._1,(x._1._2,x._2))).join(InterstStatistic).map(x=>(x._2._1._1,(x._1,x._2._1._2,x._2._2))).join(InterstStatistic).map(x=>((x._2._1._1,x._1),(x._2._1._2,x._2._1._3,x._2._2)))
    val TotalInterestFrequencyCDRHomo = TotalInterestFrequencyCDRMap.map(x=>{
      if(Type==1){
        (x._1, (x._2._1,Similarity.AdjustedCosineSimilarity4InterestKZ(x._2._2, x._2._3)))
      } else if(Type == 2){
        (x._1,(x._2._1,Similarity.CosineCoefficientSimilarity4Interest(x._2._2,x._2._3)))
      } else if(Type == 3){
        (x._1,(x._2._1,Similarity.PearsonCorrelationCoefficient4Interest(x._2._2,x._2._3)))
      } else if(Type == 4){
        (x._1,(x._2._1,Similarity.JaccardCoefficent4Interest(x._2._2,x._2._3)))
      } else if(Type == 5){
        (x._1,(x._2._1,Similarity.TanimotoCoefficent4Interest(x._2._2,x._2._3)))
      } else {
        (x._1, (x._2._1,Similarity.AdjustedCosineSimilarity4Interest(x._2._2, x._2._3)))
      }
    })
    TotalInterestFrequencyCDRHomo
  }

  /**
   *
   * @param RelationMap
   * @param InterstStatistic
   * @return
   */
  def Interest28AllSim(RelationMap:RDD[((String, String), (Array[Double], Array[Double],Array[Double],Array[Double],Array[Long]))],InterstStatistic:RDD[(String,Array[Double])])={
    val TotalInterestFrequencyCDRMap = RelationMap.map(x=>(x._1._1,(x._1._2,x._2))).join(InterstStatistic).map(x=>(x._2._1._1,(x._1,x._2._1._2,x._2._2))).join(InterstStatistic).map(x=>((x._2._1._1,x._1),(x._2._1._2,x._2._1._3,x._2._2)))
    val TotalInterestFrequencyCDRHomo = TotalInterestFrequencyCDRMap.map(x=>{
      val Adjusted = Similarity.AdjustedCosineSimilarity4Interest(x._2._2, x._2._3)
      val Cosine = Similarity.CosineCoefficientSimilarity4Interest(x._2._2,x._2._3)
      val Pearson = Similarity.PearsonCorrelationCoefficient4Interest(x._2._2,x._2._3)
      val Jaccard = Similarity.JaccardCoefficent4Interest(x._2._2,x._2._3)
      val Tanimoto = Similarity.TanimotoCoefficent4Interest(x._2._2,x._2._3)
      (x._1,(Adjusted,Cosine,Pearson,Jaccard,Tanimoto))
    })
    TotalInterestFrequencyCDRHomo
  }
  /**
   * tf-idf interest frequency
   * @param file
   * @return
   */
  def Idf4Interest(file:RDD[String])={
    val ServInterestUserNum = file.map(x=>{
      val slice =x.split(",")
      val inter = slice(InterestPosition).replaceAll("\"","")
      (slice(1),1)
    }).reduceByKey(_+_).count
    val ServInterestIDF = file.map(x=>{
      val slice =x.split(",")
      val id = slice(1)
      val inter = slice(InterestPosition).replaceAll("\"","")
      if(inter.substring(0,1).equals("0")){
        val interest = inter.substring(1,inter.length)
        ((slice(ServInterestIDPosition),interest),1)
      }else {
        val interest = inter.substring(0, inter.length)
        ((slice(ServInterestIDPosition),interest),1)
      }
    }).reduceByKey(_+_).map(x=>(x._1._2,1)).reduceByKey(_+_).map(x=>{
      val idf = math.log10(ServInterestUserNum/x._2)
      (x._1,idf)
    }).join(InterestCodeMap).map(x=>{
      (x._1,(x._2._1,x._2._2.toInt))
    })
    ServInterestIDF
  }
}