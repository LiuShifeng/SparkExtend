/**
 * Created by LiuShifeng on 2015/6/18.
 */
class userInterest {
  val treeSet = new Array[InterestTree](23)
  val size = treeSet.length
  for(i<-0 until size){
    treeSet(i) = new InterestTree
  }
  def add(s:String)={
    val treeId = s.substring(0,2).toInt
    treeSet(treeId).add(s)
  }

  def add(ui:userInterest)={
    if(size!=ui.size){

    }else{
      for(i<-0 until size){
        treeSet(i).add(ui.treeSet(i))
      }
    }
  }

  def similarity(ui:userInterest,a:Double):Double={
    0.0
  }
}
