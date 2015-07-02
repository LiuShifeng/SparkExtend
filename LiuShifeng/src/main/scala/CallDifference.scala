/**
 * Created by LiuShifeng on 2015/1/22.
 */
object CallDifference{
  /**
   * calculate the difference between two Arrays using AdjustedCosineSimilarity
   * @param a:Arrau[Double]
   * @param b:Array[Double]
   * @return Double
   */
  def timeDifference(a:Array[Double],b:Array[Double]):Double={
    val sim = Similarity.AdjustedCosineSimilarity(a,b)
    1.0-sim
  }
}
