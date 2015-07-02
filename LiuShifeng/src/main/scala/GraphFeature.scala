/**
 * Created by LiuShifeng on 2015/4/28.
 */
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.GraphLoader._
import org.apache.spark.{SparkConf, SparkContext}

object GraphFeature{
  def processDegree(k1:String, k2:String):Array[((String,Int), Double)] = {
    val res = new Array[((String,Int),Double)](4)
    res(0) = ((k1,0),1.0)
    res(1) = ((k2,1),1.0)
    if(k1.slice(0,3)=="728") res(2) = ((k2,4),1.0)
    else res(2) = ((k2,5),1.0)
    if(k2.slice(0,3)=="728") res(3) = ((k1,2),1.0)
    else res(3) = ((k1,3),1.0)
    res
  }

  def calculate(a:(Int,Int),b:(Int,Int)):(Int,Int) = {
    (a._1+b._1, a._2+b._2)
  }

  def assembleAttribute(info:Map[Int, Double]):String = {
    val res = new Array[Double](7)
    for(i <- 0 to 6){
      res(i) = info.getOrElse(i, 0.0)
    }
    res.mkString(",")
  }

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("node_attribute")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile(args(0))

    val unDigraph= rawData.flatMap { x =>
      val items = x.split(',')
      Seq((items(0) + ',' + items(5),( 1,items(3).toInt)), (items(5) + ',' + items(0),(1,items(3).toInt)))
    }.reduceByKey(calculate(_,_))
    unDigraph.map(x => x._1+'\t'+x._2._1+","+x._2._2).saveAsTextFile(args(1))

    val graphData = unDigraph.map{x => val items = x._1.split(",");(items(0),items(1))}.filter(r => r._1<r._2)
    graphData.map(x => x._1+" "+x._2).saveAsTextFile(args(2))
    val graph = edgeListFile(sc,args(2))
    val degree = graph.degrees
    val triCounts = graph.triangleCount().vertices
    val attribute = degree.join(triCounts)
    val cluCoefficient = attribute.map{x =>
      if(x._2._1>1) (x._1.toString, (6,x._2._2.toDouble*2/(x._2._1*(x._2._1-1)))) else (x._1.toString, (6,-1.0))
    }

    val data = rawData.map{x => val items = x.split(",");(items(0),items(5))}.distinct
    val netDegree = data.flatMap(x =>
      processDegree(x._1,x._2)
    ).reduceByKey(_ + _).map(x => (x._1._1,(x._1._2,x._2)))

    val att = netDegree.union(cluCoefficient).groupByKey.map(x => (x._1,x._2.toMap)).map(x => x._1+"\t"+assembleAttribute(x._2))
    att.saveAsTextFile(args(3))
  }
}

