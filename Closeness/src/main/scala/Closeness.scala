import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.math._

object Closeness{

	def NeighbourMessagge(RelationMap:RDD[((String, String), (Int, Double))])={
      val NeighbourMessage = RelationMap.map(x=>{
        val user = x._1._1
        val maxF = x._2._1
        val maxT = x._2._2
        val neighbourNum = 1
        val sumF = x._2._1
        val sumT = x._2._2
        (user,(maxF,maxT,neighbourNum,sumF,sumT))
      }).reduceByKey((x,y)=>{
        val maxF = max(x._1,y._1)
        val maxT = max(x._2,y._2)
        val neighbourNum = x._3+y._3
        val sumF = x._4+y._4
        val sumT = x._5+y._5
        (maxF,maxT,neighbourNum,sumF,sumT)
      })
      NeighbourMessage
    }
	
	def CDRCloseness(RelationMap:RDD[((String, String), (Int, Double))],Total:Boolean,Type:Int)={
      val NeighbourMessage = NeighbourMessagge(RelationMap)
      val CDRCloseness = RelationMap.map(x=>(x._1._1,(x._1._2,x._2))).join(NeighbourMessage).map(x=>{
        val user = x._1
        val neighbour = x._2._1._1
        val frequency = x._2._1._2._1.toDouble
        val duration = x._2._1._2._2
        val maxF = x._2._2._1.toDouble
        val maxT = x._2._2._2
        val neighboursNum = x._2._2._3.toDouble
        val sumF = x._2._2._4.toDouble
        val sumT = x._2._2._5
        val f = frequency/maxF
        val t = duration/maxT
        val uF = sumF/(neighboursNum*maxF)
        val uT = sumT/(neighboursNum*maxT)
        val closeness = closenessDistance(f,t)
        val R = closenessDistance(uF,uT)
        ((user,neighbour),(closeness,R,closeness/R,f,t,uF,uT))
      })
      CDRCloseness
    }
	
	def closenessDistance(f:Double,t:Double):Double = {
      val s = sqrt(pow((1-f),2)+pow((1-t),2))
      s
    }
	
    def DirectedRelationMap(file:RDD[String])={
      val DirectedMap = file.map(x => {
        val slice = x.split(",")
        if (slice(ControlPosition).equals("0")) ((slice(CallerPosition), slice(CalleePosition)), (1, slice(DurationPosition).toDouble)) else ((slice(CalleePosition), slice(CallerPosition)), (1, slice(DurationPosition).toDouble))
      }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      DirectedMap
    }
	
    def ReversedDirectedRelationMap(file:RDD[String])={
      val ReversedDirectedMap = file.map(x => {
        val slice = x.split(",")
        if (slice(ControlPosition).equals("0")) ((slice(CalleePosition), slice(CallerPosition)), (1, slice(DurationPosition).toDouble)) else ((slice(CallerPosition), slice(CalleePosition)), (1, slice(DurationPosition).toDouble))
      }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      ReversedDirectedMap
    }
	
    def UndirectedRelationMap(file:RDD[String])={
      val DirectedMap = DirectedRelationMap(file)
      val ReversedDirectedMap = ReversedDirectedRelationMap(file)
      val UndirectedMap = DirectedMap.union(ReversedDirectedMap).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      UndirectedMap
    }
	
    def BidirectedRelationMap(file:RDD[String])={
      val DirectedMap = DirectedRelationMap(file)
      val ReversedDirectedMap = ReversedDirectedRelationMap(file)
      val BidirectedMap = DirectedMap.join(ReversedDirectedMap)
      BidirectedMap
    }
	
	def main(args: Array[String]): Unit = {    
		if(args.length<2){
			System.out.println("Error!")
			System.exit(1)
		}

		val sparkConf = new SparkConf().setAppName("AdjacencyMatrix")
		val sc = new SparkContext(sparkConf)
		
		sc.stop()
	}
}