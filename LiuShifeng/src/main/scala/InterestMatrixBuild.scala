/**
 * Created by LiuShifeng on 2015/4/28.
 */
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

class  InterestMatrixBuild(val file:RDD[String],val InterestCodeMap:RDD[(String,String)]) extends Serializable{
  val acctMonthPosition = 0 //Date
  val ServInterestIDPosition = 1 //ServID
  val InterestPosition = 2 //InterestCode
  val InterestFrequencyPosition = 3 //VisitFrequency

  def getStringType(matrix:RDD[(String,Array[Int])],separator:String) ={
    val result = matrix.map(x=>{
      val l = x._2.length
      var out = x._1
      for(i<-0 until l){
        out = out + separator + x._2(i)
      }
    })
    result
  }

  def getStringTypewithoutID(matrix:RDD[(String,Array[Int])],separator:String) ={
    val result = matrix.map(x=>{
      val l = x._2.length
      var out = x._2(0).toString
      for(i<-1 until l){
        out = out + separator + x._2(i)
      }
    })
    result
  }

  def get24Dimension() ={
    val matrix = file.map(x=>{
      val slice = x.split(",")
      val inter = slice(InterestPosition).replaceAll("\"", "")
      val id = slice(ServInterestIDPosition)
      val f = slice(InterestFrequencyPosition).toInt
      if (inter.substring(0, 1).equals("0")) {
        val interest = inter.substring(1, inter.length)
        val l = interest.length
        if(l%2 == 0){
          ((id,interest.substring(0,2)),f)
        }else
          ((id,interest.substring(0,1)),f)
      } else {
        val interest = inter.substring(0, inter.length)
        val l = interest.length
        if(l%2 == 0){
          ((id,interest.substring(0,2)),f)
        }else
          ((id,interest.substring(0,1)),f)
      }
    }).reduceByKey(_+_).map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().map(x=>{
      val interestArray = new Array[Double](24)
      x._2.foreach(y=>interestArray(y._1.toInt-1) = y._2)
      (x._1,interestArray)
    })
    matrix
  }

  def get23Dimension()={
    val matrix = get24Dimension().map(x=>{
      val interestArray = x._2.slice(0,23)
      (x._1,interestArray)
    })
    matrix
  }

  def get643Dimension()={
    val statistic = file.map(x => {
      val slice = x.split(",")
      val inter = slice(InterestPosition).replaceAll("\"", "")
      if (inter.substring(0, 1).equals("0")) {
        val interest = inter.substring(1, inter.length)
        ((slice(ServInterestIDPosition), interest), slice(InterestFrequencyPosition).toInt)
      } else {
        val interest = inter.substring(0, inter.length)
        ((slice(ServInterestIDPosition), interest), slice(InterestFrequencyPosition).toInt)
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

  def get642Dimension()={
    val matrix = get643Dimension().map(x=>{
      val interestArray = new Array[Double](642)
      for(i<-0 until 23){
        interestArray(i) = x._2(i)
      }
      for(i<-24 until 643){
        interestArray(i-1) = x._2(i)
      }
      (x._1,interestArray)
    })
    matrix
  }
}