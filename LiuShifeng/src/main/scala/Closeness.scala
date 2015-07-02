import scala.math._
/**
 * Created by LiuShifeng on 2015/1/23.
 */

object Closeness{

  def closenessDistance(f:Double,t:Double):Double = {
    val s = sqrt(pow((1-f),2)+pow((1-t),2))
    s
  }
  def closenessDistanceInterval(f:Double,t:Double,i:Double):Double={
    val s = sqrt(pow((1-f),2)+pow((1-t),2)+pow(i,2))
    s
  }
  def closenessInterval(f:Double,i:Double):Double={
    val s = sqrt(pow((1-f),2)+pow(i,2))
    s
  }
  def closenessPeriodicityTimeDistribution(wa:Double,a:Double,b:Double): Double ={
    val c = wa*1+(1-wa)*b
    c
  }
  def closenessPeriodicityTimeDistribution(wa:Double,tab:Array[Long],cab:Array[Double],ca:Array[Double]):Double={
    val f = FFT.maxF(tab,86400000)
    val d = CallDifference.timeDifference(ca,cab)
    val c = wa*(1-f)+(1-wa)*d
    c
  }

  def closenessIDF(fab:Array[Double],fa:Array[Double],dab:Array[Double],da:Array[Double]):Double={
    var fValue = 0.0
    var dValue = 0.0
    val faSum = fa.sum
    val daSum = da.sum
    val fabSum = fab.sum
    val dabSum = dab.sum
    val caLength = fa.length
    val daLength = da.length
    val wca = new Array[Double](caLength)
    val wda = new Array[Double](daLength)
    for(i<- 0 until caLength){
      if (fa(i)>0.0) {
        wca(i) = log10(faSum/fa(i))
        fValue = fValue + wca(i)*fab(i)/fabSum
      }
    }
    for(i<- 0 until daLength){
      if(da(i)>0.0) {
        wda(i) = log10(daSum/da(i))
        dValue = dValue + wda(i)*dab(i)/dabSum
      }
    }
    val c = 0.5*fValue+0.5*dValue
    c
  }

  def closenessIDFDistance(fab:Array[Double],fa:Array[Double],dab:Array[Double],da:Array[Double],f:Double,t:Double):Double={
    var fValue = 0.0
    var dValue = 0.0
    val faSum = fa.sum
    val daSum = da.sum
    val fabSum = fab.sum
    val dabSum = dab.sum
    val caLength = fa.length
    val daLength = da.length
    val wca = new Array[Double](caLength)
    val wda = new Array[Double](daLength)
    for(i<- 0 until caLength){
      if (fa(i)>0.0) {
        wca(i) = log10(faSum/fa(i))
        fValue = fValue + wca(i)*fab(i)/fabSum
      }
    }
    for(i<- 0 until daLength){
      if(da(i)>0.0) {
        wda(i) = log10(daSum/da(i))
        dValue = dValue + wda(i)*dab(i)/dabSum
      }
    }
    val s = sqrt(pow((1-f),2)*fValue+pow((1-t),2)*dValue)
    s
  }
}
