/**
 * Created by LiuShifeng on 2015/3/4.
 */
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

class Clustering() extends Serializable{
  var numClusters:Int = 1
  var numIterations:Int = 1
  var WSSSE:Double = -1.0
  var KmeansModel:KMeansModel = null

  def run(file: RDD[String])={
    val parseData = file.map(s => Vectors.dense(s.split(',').map(_.toDouble)))
    if(numClusters > 0 && numIterations > 0 && parseData != null){
      val clusters = KMeans.train(parseData, numClusters, numIterations)
      WSSSE = clusters.computeCost(parseData)
      KmeansModel = clusters
    } else{
      println("ERROR!")
    }
  }

  def run(file:RDD[String],numC:Int,numI:Int) ={
    numClusters = numC
    numIterations = numI
    val parseData = file.map(s => Vectors.dense(s.split(',').map(_.toDouble)))
    if(numClusters > 0 && numIterations > 0 && parseData != null){
      val clusters = KMeans.train(parseData, numClusters, numIterations)
      WSSSE = clusters.computeCost(parseData)
      KmeansModel = clusters
    } else{
      println("ERROR!")
    }
  }
}
