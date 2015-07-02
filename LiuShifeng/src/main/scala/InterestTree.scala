/**
 * Created by LiuShifeng on 2015/6/18.
 */
class InterestTree {
  val MAXSIZE = 100
  val layerSet = new Array[Array[Double]](4)
  val size = layerSet.length
  for(i<- 0 until size){
    layerSet(i) = new Array[Double](MAXSIZE)
  }

  def add(s:String)={

  }

  def add(it:InterestTree)={
    if((size!=it.size)||(MAXSIZE!=it.MAXSIZE)){

    }else{
      for(i<-0 until size){
        for(j<- 0 until MAXSIZE){
          layerSet(i)(j) = layerSet(i)(j) + it.layerSet(i)(j)
        }
      }
    }
  }
}
