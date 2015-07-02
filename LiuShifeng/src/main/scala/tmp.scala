import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
/**
 * Created by LiuShifeng on 2015/4/14.
 */
/**
object tmp{
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("AttributeBUild").set("spark.akka.heartbeat.pauses", "1200").set("spark.akka.failure-detector.threshold", "600").set("spark.akka.heartbeat.interval", "2000")
    val sc = new SparkContext(sparkConf)

    val CDRFilePath = "hdfs://dell01:12306/user/tele/data/nanning/CDR/wash_CDR_NanNing_201409.csv"
    val ServInterestFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201407.csv"
    val TerminalFilePath = "hdfs://dell01:12306/user/tele/data/nanning/Terminal/Terminal-NanNing-201409.csv"
    val ServInfoFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInfo/washed_Serv_Info-NanNing-1409.csv"

    val CDRFile = sc.textFile(CDRFilePath)
    val ServInterestFile = sc.textFile(ServInterestFilePath)
    val TerminalFile = sc.textFile(TerminalFilePath)
    val ServInfoFile = sc.textFile(ServInfoFilePath)

    val callerPosition = 0
    val calleePosition = 5
    val CDRID = CDRFile.map(x=>{
      val s = x.split(",")
      val r = new Array[String](2)
      r(0) = s(callerPosition)
      r(1) = s(calleePosition)
      r
    }).flatMap(x=>x).map(x=>(x,1)).reduceByKey(_+_).filter(x=> !x._1.contains("-")).map(x=>(x._1,1))

    val ServInterestIDPosition = 1
    val InterestPosition = 2
    val InterestFrequencyPosition = 3
    val ServInterestID = ServInterestFile.map(x=>{
      val slice = x.split(",")
      val inter = slice(InterestPosition).replaceAll("\"", "")
      val id = slice(ServInterestIDPosition)
      val f = slice(InterestFrequencyPosition).toInt
      if (inter.substring(0, 1).equals("0")) {
        val interest = inter.substring(1, inter.length)
        val l = interest.length
        if(l%2 == 0){
          ((id,interest.substring(0,2)),f)
        }else
          ((id,interest.substring(0,1)),f)
      } else {
        val interest = inter.substring(0, inter.length)
        val l = interest.length
        if(l%2 == 0){
          ((id,interest.substring(0,2)),f)
        }else
          ((id,interest.substring(0,1)),f)
      }
    }).reduceByKey(_+_).map(x=>(x._1._1,(x._1._2,x._2))).groupByKey().map(x=>{
      val interestArray = new Array[Int](24)
      x._2.foreach(y=>interestArray(y._1.toInt-1) = y._2)
      (x._1,interestArray)
    })
    ServInterestID.map(x=>{
      var out = x._1
      for(i<-0 until 23){
        out = out +" " + x._2(i)
      }
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/Interest/JulInterestFrequencyMatrix_IDData")
    ServInterestID.map(x=>{
      var out = x._2(0).toString
      for(i<-1 until 23){
        out = out + " " + x._2(i)
      }
      out
    }).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/Interest/JulInterestFrequencyMatrix_Data")


    val percent = 0.5
    val percentInterestNum = ServInterestID.map(x=> {
      val interestArray = new Array[Int](23)
      for (i <- 0 until 23) {
        interestArray(i) = x._2(i)
      }
      val sorted = interestArray.sorted.reverse
      val sum = sorted.sum + 1
      var count = 0
      var cal = 0
      for(i<-0 until 23){
        if((cal.toDouble/sum)< percent){
          cal = cal + sorted(i)
          count = count + 1
        }
      }
      (count,1)
    }).reduceByKey(_+_)
    percentInterestNum.count
    percentInterestNum.take(10)

    //ACCT_MONTH,SERV_ID,MEID_CODE,IMSI_CODE,BRAND_CODE,BRAND_NAME,MOBILE_TERMINAL_CODE,MOBILE_TERMINAL_NAME,IFSP_DW,IFSP_OS,IFSP_EVDO,PRICE_SCOPE,RPICE,LOG_TIME
    val TerminalIDPosition = 1
    val TerminalBrandCodePosition = 4
    val TerminalCodePosition = 6
    val DWPosition = 8
    val TerminalOSPosition = 9
    val TerminalEVDOPosition = 10
    val TerminalPricePosition = 12
    val TerminalLogTimePosition = 13
    val TerminalID = TerminalFile.map(x=>{
      val s = x.split(",")
      val id = s(TerminalIDPosition)
      val brand = s(TerminalBrandCodePosition)
      val mobileCode = s(TerminalCodePosition)
      val DW = s(DWPosition)
      val os = s(TerminalOSPosition)
      val EVDO = s(TerminalEVDOPosition)
      val price = s(TerminalPricePosition)
      val logTime = s(TerminalLogTimePosition)
      (id,(brand,mobileCode,DW,os,EVDO,price,logTime))
    })

    val ServInfoIDPosition = 2
    val ServInfoID = ServInfoFile.map(x=>{
    })

    val IdInfo = CDRID.leftOuterJoin(ServInterestID).map(x=>{
      val empty = new Array[Int](24)
      for(i<-0 until 24){
        empty(i) = 0
      }
      (x._1,x._2._2 match {case Some(z) => z;case None => empty})
    }).leftOuterJoin(TerminalID).map(x=>{
      (x._1,(x._2._1,x._2._2 match {case Some(z) => z;case None => ("X","X","X","X","X","X")}))
    })
    IdInfo.filter(x=>x._2._2.equals(("X","X","X","X","X","X"))).take(10)

    val ServIDFilePath = "hdfs://dell01:12306/user/tele/data/nanning/4G_ServID.csv"
    val MarketingFilePath = "hdfs://dell01:12306/user/tele/data/nanning/Marketing/Marketing-NanNing-201410.csv"

    val ServIDFile = sc.textFile(ServIDFilePath)
    val MarketingFile = sc.textFile(MarketingFilePath)

    val ServIdMap = ServIDFile.map(x=>{
      val s = x.split(",")
      (s(0),s(1))
    })
    val acctMonthPosition = 0
    val servIDPosition = 1
    val productOfferInstanceIDPostion = 4
    val productOfferIDPosition = 6
    val expDatePosition = 9
    val effDatePosition = 10
    val offerCompletedDatePosition = 11
    val MarketingMap = MarketingFile.map(x=>{
      val s = x.split(",")
      val id = s(servIDPosition)
      val productOfferInstance = s(productOfferInstanceIDPostion)
      val productOfferId = s(productOfferIDPosition)
      val expDate = s(expDatePosition)
      val effDate = s(effDatePosition)
      (id,(productOfferInstance,productOfferId,expDate,effDate))
    })

    val servIdMarketInfo = ServIdMap.join(MarketingMap).map(x=>(x._1,(x._2._1,x._2._2._1,x._2._2._2,x._2._2._3,x._2._2._4)))

    val JulServInfoFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInfo/washed_Serv_Info-NanNing-201407.csv"
    val AugServInfoFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInfo/washed_Serv_Info-NanNing-201408.csv"
    val SepServInfoFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInfo/washed_Serv_Info-NanNing-201409.csv"
    val OctServInfoFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInfo/washed_Serv_Info-NanNing-201410.csv"

    val JulServInfoFile = sc.textFile(JulServInfoFilePath)
    val AugServInfoFile = sc.textFile(AugServInfoFilePath)
    val SepServInfoFile = sc.textFile(SepServInfoFilePath)
    val OctServInfoFile = sc.textFile(OctServInfoFilePath)

    val ServInfoIdPosition = 2
    val MainOfferIdPosition = 31
    val MainOfferSubclassPosition = 32
    val JulServInfo_4G = JulServInfoFile.map(x=>{
      val s = x.split(",")
      (s(ServInfoIdPosition),(7,s(MainOfferIdPosition),s(MainOfferSubclassPosition)))
    }).filter(x=>x._2._2.contains("280018182"))
    val AugServInfo_4G = AugServInfoFile.map(x=>{
      val s = x.split(",")
      (s(ServInfoIdPosition),(8,s(MainOfferIdPosition),s(MainOfferSubclassPosition)))
    }).filter(x=>x._2._2.contains("280018182"))
    val SepServInfo_4G = SepServInfoFile.map(x=>{
      val s = x.split(",")
      (s(ServInfoIdPosition),(9,s(MainOfferIdPosition),s(MainOfferSubclassPosition)))
    }).filter(x=>x._2._2.contains("280018182"))
    val OctServInfo_4G = OctServInfoFile.map(x=>{
      val s = x.split(",")
      (s(ServInfoIdPosition),(10,s(MainOfferIdPosition),s(MainOfferSubclassPosition)))
    }).filter(x=>x._2._2.contains("280018182"))


  }
}
*/