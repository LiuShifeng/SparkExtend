import org.apache.spark.rdd.RDD

/**
 * Created by LiuShifeng on 2015/5/15.
 */

class NetFlowFeature(val file:RDD[String]) extends Serializable{
  def getFeature() ={
    val feature = file.map(x=>{
      val netFlow = new NetFlow(x)
      netFlow.getFeature()
    })
    feature
  }
}
