import scala.collection.mutable.ArrayBuffer

/**
 * Created by LiuShifeng on 2015/6/18.
 */
class TreeNode {
  var code:String = ""
  var frequency:Double = 0.0
  var layer:Int = 0
  var children = new ArrayBuffer[TreeNode]()
}
