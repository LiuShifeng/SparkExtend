import scala.math._
/**
 * Created by LiuShifeng on 2015/1/22.
 */
object FFT {
  /**
   * input:timestamp:Long,interval;Long
   * statistic the frequency in each interval
   * @param a:Array[Long] timestamp
   * @param interval:Long timeInterval
   * @return
   */
  def patchZero(a:Array[Long],interval:Long):Array[Double]={
    val last = a.max
    val first = a.min
    val length = ((last-first)/interval).toInt+1
    val result = new Array[Double](length)
    val aLength = a.length
    for(i<-0 until aLength){
      val p = ((a(i)-first)/interval).toInt
      result(p) = result(p)+1.0
    }
    result
  }

  /**
   * input:frequency in each interval
   * calculate the maxFrequency using FFT
   * @param a;Array[Double]
   * @return Double maxFrequency
   */
  def maxF(a:Array[Double]):Double={
    val aLength = a.length
    val mean = a.sum/aLength
    val e = new Array[Double](aLength)
    val k = new Array[Double](aLength)
    for(i<-0 until aLength){
      e(i) = a(i) - mean
      k(i) = (i+1).toDouble
    }
    val f = new Array[Double](100)
    for(i<-0 until 100){
      f(i) = i*0.01
    }
    val S = new Array[Double](100)
    val C = new Array[Double](100)
    val I = new Array[Double](100)
    for(i<-0 until 100){
      var sumS = 0.0
      var sumC = 0.0
      for(j<-0 until aLength){
        sumS = sumS + e(j)*sin(2*Pi*f(i)*k(j))
        sumC = sumC + e(j)*cos(2*Pi*f(i)*k(j))
      }
      S(i) = sumS
      C(i) = sumC
      I(i) = C(i)*C(i)+S(i)*S(i)
    }
    val maxf = f(I.indexOf(I.max))
    maxf
  }

  /**
   * input:timestam:Long,interval;Long
   * calculate the maxFrequency using FFT
   * @param a:Array[Long] timestamp
   * @param interval:Long timeInterval
   * @return Double maxFrequency
   */
  def maxF(a:Array[Long],interval:Long):Double={
    maxF(patchZero(a,interval))
  }
}
