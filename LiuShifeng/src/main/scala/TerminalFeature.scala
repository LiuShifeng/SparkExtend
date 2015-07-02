/**
 * Created by LiuShifeng on 2015/4/28.
 */

import org.apache.spark.rdd.RDD

class TerminalFeature(val file:RDD[String]) extends Serializable{
  def getStringType(matrix:RDD[(String,Array[Int])]) ={
    val result = matrix.map(x=>x._1+","+x._2.mkString(","))
    result
  }

  def getFeature() ={
    val feature = file.map(x=>{
      val terminal = new Terminal(x)
      terminal.getFeature()
    })
    feature
  }
}
