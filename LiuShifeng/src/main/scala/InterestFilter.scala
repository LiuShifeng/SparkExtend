import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

//LogisticRegression
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
//Random Forest
import org.apache.spark.mllib.tree.RandomForest
/**
 * Created by LiuShifeng on 2015/6/4.
 */

object InterestFilter{
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

    val sparkConf = new SparkConf().setAppName("LiuShifeng_InterestFilter").set("spark.akka.heartbeat.pauses", "1200").set("spark.akka.failure-detector.threshold", "600").set("spark.akka.heartbeat.interval", "2000")
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
    val Interest23D = Interest.get23Dimension()

    val InterestDistribution = CDRMap.map(x=>(x._1._1,1)).reduceByKey(_+_).join(Interest23D).map(x=>(x._1,x._2._2)).filter(x=>x._2.sum!=0)
//    InterestDistribution.map(x=>{
//      val l = x._2.length
//      var out = x._1
//      for(i<-0 until l){
//        out = out + "," + x._2(i)
//      }
//      out
//    }).saveAsTextFile(InterestDistributionDestPath)

    val InterestNoneArray = new Array[Double](23)
    val CDRInterestMap = CDRMap.map(x=>(x._1._1,(x._1._2,x._2))).join(InterestDistribution).map(x=>(x._2._1._1,(x._1,x._2._2,x._2._1._2))).join(InterestDistribution).map(x=>((x._2._1._1,x._1),(x._2._1._2,x._2._2,x._2._1._3)))
    val CDRInterestReleventMap = CDRMap.map(x=>(x._1._1,(x._1._2,x._2))).leftOuterJoin(InterestDistribution).map(x=>(x._2._1._1,(x._1,x._2._2 match{ case Some(z) => z;case None =>InterestNoneArray},x._2._1._2))).leftOuterJoin(InterestDistribution).map(x=>((x._2._1._1,x._1),(x._2._1._2,x._2._2 match{ case Some(z) => z;case None =>InterestNoneArray},x._2._1._3)))

    val CDRInterestSimilarityMap = CDRInterestMap.map(x=>(x._1,(x._2._1,x._2._2,x._2._3,Similarity.AdjustedCosineSimilarity(x._2._1,x._2._2))))
    val CDRInterestSimilarityMap_S06 = CDRInterestSimilarityMap.filter(x=>x._2._4>=0.6)
    val CDRInterestSimilarityMap_S07 = CDRInterestSimilarityMap.filter(x=>x._2._4>=0.7)
    val CDRInterestSimilarityMap_S08 = CDRInterestSimilarityMap.filter(x=>x._2._4>=0.8)
    val CDRInterestSimilarityMap_S09 = CDRInterestSimilarityMap.filter(x=>x._2._4>=0.9)

//    CDRInterestSimilarityMap_S06.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution + "," + x._2._4
//      out
//    }).saveAsTextFile(CDRInterestFilterDestPath+"_S06")
//    CDRInterestSimilarityMap_S07.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution + "," + x._2._4
//      out
//    }).saveAsTextFile(CDRInterestFilterDestPath+"_S07")
//    CDRInterestSimilarityMap_S08.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution + "," + x._2._4
//      out
//    }).saveAsTextFile(CDRInterestFilterDestPath+"_S08")
//    CDRInterestSimilarityMap_S09.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution + "," + x._2._4
//      out
//    }).saveAsTextFile(CDRInterestFilterDestPath+"_S09")

    val CDRInterestReleventID_S06 = CDRInterestSimilarityMap_S06.map(x=>(x._1._1,1)).reduceByKey(_+_)
    val CDRInterestReleventID_S07 = CDRInterestSimilarityMap_S07.map(x=>(x._1._1,1)).reduceByKey(_+_)
    val CDRInterestReleventID_S08 = CDRInterestSimilarityMap_S08.map(x=>(x._1._1,1)).reduceByKey(_+_)
    val CDRInterestReleventID_S09 = CDRInterestSimilarityMap_S09.map(x=>(x._1._1,1)).reduceByKey(_+_)

    val CDRInterestReleventMap_S06 = CDRInterestSimilarityMap.map(x=>(x._1._1,(x._1._2,x._2))).join(CDRInterestReleventID_S06).map(x=>((x._1,x._2._1._1),x._2._1._2))
    val CDRInterestReleventMap_S07 = CDRInterestSimilarityMap.map(x=>(x._1._1,(x._1._2,x._2))).join(CDRInterestReleventID_S07).map(x=>((x._1,x._2._1._1),x._2._1._2))
    val CDRInterestReleventMap_S08 = CDRInterestSimilarityMap.map(x=>(x._1._1,(x._1._2,x._2))).join(CDRInterestReleventID_S08).map(x=>((x._1,x._2._1._1),x._2._1._2))
    val CDRInterestReleventMap_S09 = CDRInterestSimilarityMap.map(x=>(x._1._1,(x._1._2,x._2))).join(CDRInterestReleventID_S09).map(x=>((x._1,x._2._1._1),x._2._1._2))

//    CDRInterestReleventMap_S06.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution + "," + x._2._4
//      out
//    }).saveAsTextFile(CDRInterestFilterReleventDestPath+"_S06")
//    CDRInterestReleventMap_S07.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution + "," + x._2._4
//      out
//    }).saveAsTextFile(CDRInterestFilterReleventDestPath+"_S07")
//    CDRInterestReleventMap_S08.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution + "," + x._2._4
//      out
//    }).saveAsTextFile(CDRInterestFilterReleventDestPath+"_S08")
//    CDRInterestReleventMap_S09.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution + "," + x._2._4
//      out
//    }).saveAsTextFile(CDRInterestFilterReleventDestPath+"_S09")

    val CDRInterestReleventMap_RS06 = CDRInterestReleventMap.map(x=>(x._1._1,(x._1._2,x._2))).join(CDRInterestReleventID_S06).map(x=>((x._1,x._2._1._1),x._2._1._2))
    val CDRInterestReleventMap_RS07 = CDRInterestReleventMap.map(x=>(x._1._1,(x._1._2,x._2))).join(CDRInterestReleventID_S07).map(x=>((x._1,x._2._1._1),x._2._1._2))
    val CDRInterestReleventMap_RS08 = CDRInterestReleventMap.map(x=>(x._1._1,(x._1._2,x._2))).join(CDRInterestReleventID_S08).map(x=>((x._1,x._2._1._1),x._2._1._2))
    val CDRInterestReleventMap_RS09 = CDRInterestReleventMap.map(x=>(x._1._1,(x._1._2,x._2))).join(CDRInterestReleventID_S09).map(x=>((x._1,x._2._1._1),x._2._1._2))

//    CDRInterestReleventMap_RS06.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution
//      out
//    }).saveAsTextFile(CDRInterestFilterReleventDestPath+"_RS06")
//    CDRInterestReleventMap_RS07.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution
//      out
//    }).saveAsTextFile(CDRInterestFilterReleventDestPath+"_RS07")
//    CDRInterestReleventMap_RS08.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution
//      out
//    }).saveAsTextFile(CDRInterestFilterReleventDestPath+"_RS08")
//    CDRInterestReleventMap_RS09.map(x=>{
//      val AI = x._2._1.mkString(",")
//      val BI = x._2._2.mkString(",")
//      val frequencyHourDistribution = x._2._3._1.mkString(",")
//      val frequencyWeekDistribution = x._2._3._2.mkString(",")
//      val durationHourDistribution = x._2._3._3.mkString(",")
//      val durationWeekDistribution = x._2._3._4.mkString(",")
//      val out = x._1._1 + "," + x._1._2 + "," + AI + "," + BI + "," + frequencyHourDistribution + "," + frequencyWeekDistribution + "," + durationHourDistribution + "," + durationWeekDistribution
//      out
//    }).saveAsTextFile(CDRInterestFilterReleventDestPath+"_RS09")

    /**
     * MLib Data Preparation
     */
    val data = CDRInterestSimilarityMap.map(x=>{
      val label = if (x._2._4 < 0) 0 else if(x._2._4 >= 1.toDouble) 9 else (x._2._4*10).toInt
      var feature = new ArrayBuffer[Double]()
      feature ++= x._2._3._1
      feature ++= x._2._3._2
      feature ++= x._2._3._3
      feature ++= x._2._3._4
      LabeledPoint(label.toDouble, Vectors.dense(feature.toArray))
    })
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    /**
     * LogisticRegression
     */
    // Run training algorithm to build the model
    val LogisticRegressionmodel = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = LogisticRegressionmodel.predict(features)
      (prediction, label)
    }

    val precision = predictionAndLabels.map(x=>if(x._1-x._2<=1 && x._2-x._1<=1) ("T",(1,1)) else ("T",(0,1))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>x._2._1.toDouble/x._2._2.toDouble).take(1)(0)

//    // Get evaluation metrics.
//    val metrics = new MulticlassMetrics(predictionAndLabels)
//    val precision = metrics.precision
    println("Logistic Regression Precision = " + precision)

    // Save and load model
//    LogisticRegressionmodel.save(sc, "hdfs://dell01:12306/user/tele/LiuShifeng/InterestFilter/Jan/LogisticRegressionModel")
//    val sameModel = LogisticRegressionModel.load(sc, "myModelPath")

    /**
     * Random Forest
     */
    // Train a RandomForest model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 10
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 10 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 6
    val maxBins = 32

    val RandomForestmodel = RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = test.map { point =>
      val prediction = RandomForestmodel.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => (r._1-r._2>1)||(r._2-r._1>1)).count.toDouble / test.count()
    println("Random Forest Precision = " + (1-testErr))
    println("Test Error = " + testErr)
//    println("Learned classification forest model:\n" + RandomForestmodel.toDebugString)

    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = RandomForestModel.load(sc, "myModelPath")

    sc.stop()
  }
}