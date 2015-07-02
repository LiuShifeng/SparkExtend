/**
 * Created by LiuShifeng on 2015/5/19.
 */
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object AttributeBuild{
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("AttributeBuild").set("spark.akka.heartbeat.pauses", "1200").set("spark.akka.failure-detector.threshold", "600").set("spark.akka.heartbeat.interval", "2000")
    val sc = new SparkContext(sparkConf)
    /**
    val interestAttibuteFilePath = ""
    val interestAttributeFile = sc.textFile(interestAttibuteFilePath)

    val interestAttributeMap = interestAttributeFile.map(x=>{
      val s = x.split(",")
      val interestAttribute = new Array[Double](23)
      for(i<-0 until 23){
        interestAttribute(i) = s(i+2).toDouble
      }
      val lineID = s(0).toInt
      val userID = s(1)
      (lineID,(userID,interestAttribute))
    })

    val clusterFilePath = ""
    val clusterFile = sc.textFile(clusterFilePath)
    val clusterMap = clusterFile.map(x=>{
      val s = x.split(",")
      val clusterID = s(0).toInt
      val length = s.length-1
      val clusterArray = new Array[(Int,Int)](length)
      for(i<-0 until length){
        clusterArray(i) = (s(i+1).toInt,clusterID)
      }
      clusterArray//(lineID,clusterID)
    }).flatMap(x=>x)

    val userInterestInfo = interestAttributeMap.leftOuterJoin(clusterMap).map(x=>(x._2._1._1,(x._2._2 match {case Some(z) => z;case None => -1},x._2._1._2)))//ID,cluserID,interestAttribute
      **/
    /**
    val userCDRFilePath = ""
    val userCDRFile = sc.textFile(userCDRFilePath)
    val userCDRProfile = new CDRProfile(userCDRFile)
    val userCallTimeDistribution = userCDRProfile.getTimeDistribution()
    val userNodeFeature = userCDRProfile.getNodeFeature()

    val userConsumeFilePath = ""
    val userConsumeFile = sc.textFile(userConsumeFilePath)
    val UserBasicProfile = new UserBasicProfile(userConsumeFile)
    val userConsumeInfo = UserBasicProfile.getUserBasicProfile()
      */
    val CodePosition = 0 //兴趣点编码
    val NamePosition = 1 //兴趣点名称
    val ParentCodePostion = 2 //父目录节点
    val LevelPosition = 3 //所属目录层级
    val VectorPosition = 4 //VectorId
    val userInterestCodeFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-Code-NanNing.csv"
    val userInterestCodeFile = sc.textFile(userInterestCodeFilePath)
    val userInterestCodeMap = userInterestCodeFile.map(x => {
      val slice = x.split(",")
      (slice(CodePosition), slice(VectorPosition))
    })
    /**
    val userInterestFilePath = ""
    val userInterestFile = sc.textFile(userInterestFilePath)
    val userInterest = new InterestMatrixBuild(userInterestFile,userInterestCodeMap)
    val userInterset23 = userInterest.get23Dimension()

    val userTerminalFilePath = ""
    val userTerminalFile = sc.textFile(userTerminalFilePath)
    val userTerminal = new TerminalFeature(userTerminalFile)
    val userTerminalFeature = userTerminal.getFeature()

    val userMarketFilePath = ""
    val userMarketFile = sc.textFile(userMarketFilePath)
    val userMarket = new UserMarketProfile(userMarketFile)
    val userMarketFeature = userMarket.getUserProfile()
      **/
    val JulInterestFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201407.csv"
    val AugInterestFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201408.csv"
    val SepInterestFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201409.csv"
    val OctInterestFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInterest/ServInterest-NanNing-201410.csv"
    val JulInterestFile = sc.textFile(JulInterestFilePath)
    val AugInterestFile = sc.textFile(AugInterestFilePath)
    val SepInterestFile = sc.textFile(SepInterestFilePath)
    val OctInterestFile = sc.textFile(OctInterestFilePath)
    val JulInterest = new InterestMatrixBuild(JulInterestFile,userInterestCodeMap)
    val AugInterest = new InterestMatrixBuild(AugInterestFile,userInterestCodeMap)
    val SepInterest = new InterestMatrixBuild(SepInterestFile,userInterestCodeMap)
    val OctInterest = new InterestMatrixBuild(OctInterestFile,userInterestCodeMap)
    val JulInterestDimension23 = JulInterest.get24Dimension()
    val AugInterestDimension23 = AugInterest.get24Dimension()
    val SepInterestDimension23 = SepInterest.get24Dimension()
    val OctInterestDimension23 = OctInterest.get24Dimension()

    val JulServInfoFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInfo/washed_Serv_Info-NanNing-201407.csv"
    val AugServInfoFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInfo/washed_Serv_Info-NanNing-201408.csv"
    val SepServInfoFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInfo/washed_Serv_Info-NanNing-201409.csv"
    val OctServInfoFilePath = "hdfs://dell01:12306/user/tele/data/nanning/ServInfo/washed_Serv_Info-NanNing-201410.csv"
    val JulServInfoFile = sc.textFile(JulServInfoFilePath)
    val AugServInfoFile = sc.textFile(AugServInfoFilePath)
    val SepServInfoFile = sc.textFile(SepServInfoFilePath)
    val OctServInfoFile = sc.textFile(OctServInfoFilePath)
    val JulUserBasicProfile = new UserBasicProfile(JulServInfoFile).getUserBasicProfile()
    val AugUserBasicProfile = new UserBasicProfile(AugServInfoFile).getUserBasicProfile()
    val SepUserBasicProfile = new UserBasicProfile(SepServInfoFile).getUserBasicProfile()
    val OctUserBasicProfile = new UserBasicProfile(OctServInfoFile).getUserBasicProfile()

    val JulUserMessage = JulInterestDimension23.leftOuterJoin(JulUserBasicProfile).map(x=>{
      (x._1,(x._2._1,x._2._2 match {case Some(z) => z;case None => {
        val userAttribute = ArrayBuffer[String]()
        val userCallAttribute = ArrayBuffer[Double]()
        val userNetAttribute = ArrayBuffer[Double]()
        userAttribute += "X"//s(SERV_STATUS)
        userAttribute += "X"//s(WLAN_USE_FLAG)
        userAttribute += "X"//s(CDMA_STATUS)
        userAttribute += "X"//s(OFFER_FLAG)
        userAttribute += "X"//s(EVDO_FLAG)
        userAttribute += "X"//s(FLAG_3G)
        userAttribute += "X"//s(INNET_LENGTH)
        userAttribute += "X"//s(MAIN_OFFER_ID)
        userAttribute += "X"//s(CUST_EDUCATION_LEVEL)
        userAttribute += "X"//s(CUST_GENDER)
        userAttribute += "X"//s(AGE)
        userAttribute += "X"//s(MARRY_STATUS)
        userAttribute += "X"//s(CUST_NATIONALITY)
        userAttribute += "X"//s(BIRTH_DATE)
        userAttribute += "X"//s(MAIN_OFFER_GRADE)
        userAttribute += "X"//s(STAFF_FLAG)
        userAttribute += "X"//s(ARPU_AVERAGE)
        userAttribute += "X"//s(ARPU)

        userCallAttribute += -1//s(DURATION_CALLING).toDouble
        userCallAttribute += -1//s(OUTNET_DURATION_RATE).toDouble
        userCallAttribute += -1//s(CALLING_NUM).toDouble
        userCallAttribute += -1//s(CALLNUM_DROP_RATE).toDouble
        userCallAttribute += -1//s(BILL_CHANGE_RATE).toDouble
        userCallAttribute += -1//s(DURATION_5).toDouble
        userCallAttribute += -1//s(DURATION_17).toDouble
        userCallAttribute += -1//s(DURATION_18).toDouble
        userCallAttribute += -1//s(DURATION_19).toDouble
        userCallAttribute += -1//s(DURATION_16).toDouble
        userCallAttribute += -1//s(DURATION_20).toDouble
        userCallAttribute += -1//s(DURATION_3).toDouble
        userCallAttribute += -1//s(DURATION_1).toDouble
        userCallAttribute += -1//s(DURATION_2).toDouble
        userCallAttribute += -1//s(DURATION_22).toDouble
        userCallAttribute += -1//s(DURATION_23).toDouble
        userCallAttribute += -1//s(DURATION_24).toDouble
        userCallAttribute += -1//s(DURATION_21).toDouble
        userCallAttribute += -1//s(DURATION_25).toDouble
        userCallAttribute += -1//s(DURATION_8).toDouble
        userCallAttribute += -1//s(DURATION_26).toDouble
        userCallAttribute += -1//s(DURATION_27).toDouble
        userCallAttribute += -1//s(DURATION_11).toDouble
        userCallAttribute += -1//s(DURATION_12).toDouble
        userCallAttribute += -1//s(DURATION_14).toDouble
        userCallAttribute += -1//s(DURATION_15).toDouble
        userCallAttribute += -1//s(CALLING_NUM_COUNT).toDouble
        userCallAttribute += -1//s(ALL_CALL_DURATION).toDouble
        userCallAttribute += -1//s(DURATION_4).toDouble
        userCallAttribute += -1//s(DURATION_CALLED).toDouble
        userCallAttribute += -1//s(CALLED_NUM).toDouble
        userCallAttribute += -1//s(DURATION_34).toDouble
        userCallAttribute += -1//s(DURATION_33).toDouble
        userCallAttribute += -1//s(DURATION_32).toDouble
        userCallAttribute += -1//s(DURATION_29).toDouble
        userCallAttribute += -1//s(DURATION_31).toDouble
        userCallAttribute += -1//s(DURATION_28).toDouble


        userNetAttribute += -1//s(NET_FLOW).toDouble
        userNetAttribute += -1//s(REMOTE_NET_DURATION).toDouble
        userNetAttribute += -1//s(LOCAL_NET_DURATION).toDouble
        (userAttribute,userCallAttribute,userNetAttribute)
      }}))
    }).map(x=>(x._1,(x._2._1,x._2._2._1,x._2._2._2,x._2._2._3)))
    val AugUserMessage = AugInterestDimension23.leftOuterJoin(AugUserBasicProfile).map(x=>{
      (x._1,(x._2._1,x._2._2 match {case Some(z) => z;case None => {
        val userAttribute = ArrayBuffer[String]()
        val userCallAttribute = ArrayBuffer[Double]()
        val userNetAttribute = ArrayBuffer[Double]()
        userAttribute += "X"//s(SERV_STATUS)
        userAttribute += "X"//s(WLAN_USE_FLAG)
        userAttribute += "X"//s(CDMA_STATUS)
        userAttribute += "X"//s(OFFER_FLAG)
        userAttribute += "X"//s(EVDO_FLAG)
        userAttribute += "X"//s(FLAG_3G)
        userAttribute += "X"//s(INNET_LENGTH)
        userAttribute += "X"//s(MAIN_OFFER_ID)
        userAttribute += "X"//s(CUST_EDUCATION_LEVEL)
        userAttribute += "X"//s(CUST_GENDER)
        userAttribute += "X"//s(AGE)
        userAttribute += "X"//s(MARRY_STATUS)
        userAttribute += "X"//s(CUST_NATIONALITY)
        userAttribute += "X"//s(BIRTH_DATE)
        userAttribute += "X"//s(MAIN_OFFER_GRADE)
        userAttribute += "X"//s(STAFF_FLAG)
        userAttribute += "X"//s(ARPU_AVERAGE)
        userAttribute += "X"//s(ARPU)

        userCallAttribute += -1//s(DURATION_CALLING).toDouble
        userCallAttribute += -1//s(OUTNET_DURATION_RATE).toDouble
        userCallAttribute += -1//s(CALLING_NUM).toDouble
        userCallAttribute += -1//s(CALLNUM_DROP_RATE).toDouble
        userCallAttribute += -1//s(BILL_CHANGE_RATE).toDouble
        userCallAttribute += -1//s(DURATION_5).toDouble
        userCallAttribute += -1//s(DURATION_17).toDouble
        userCallAttribute += -1//s(DURATION_18).toDouble
        userCallAttribute += -1//s(DURATION_19).toDouble
        userCallAttribute += -1//s(DURATION_16).toDouble
        userCallAttribute += -1//s(DURATION_20).toDouble
        userCallAttribute += -1//s(DURATION_3).toDouble
        userCallAttribute += -1//s(DURATION_1).toDouble
        userCallAttribute += -1//s(DURATION_2).toDouble
        userCallAttribute += -1//s(DURATION_22).toDouble
        userCallAttribute += -1//s(DURATION_23).toDouble
        userCallAttribute += -1//s(DURATION_24).toDouble
        userCallAttribute += -1//s(DURATION_21).toDouble
        userCallAttribute += -1//s(DURATION_25).toDouble
        userCallAttribute += -1//s(DURATION_8).toDouble
        userCallAttribute += -1//s(DURATION_26).toDouble
        userCallAttribute += -1//s(DURATION_27).toDouble
        userCallAttribute += -1//s(DURATION_11).toDouble
        userCallAttribute += -1//s(DURATION_12).toDouble
        userCallAttribute += -1//s(DURATION_14).toDouble
        userCallAttribute += -1//s(DURATION_15).toDouble
        userCallAttribute += -1//s(CALLING_NUM_COUNT).toDouble
        userCallAttribute += -1//s(ALL_CALL_DURATION).toDouble
        userCallAttribute += -1//s(DURATION_4).toDouble
        userCallAttribute += -1//s(DURATION_CALLED).toDouble
        userCallAttribute += -1//s(CALLED_NUM).toDouble
        userCallAttribute += -1//s(DURATION_34).toDouble
        userCallAttribute += -1//s(DURATION_33).toDouble
        userCallAttribute += -1//s(DURATION_32).toDouble
        userCallAttribute += -1//s(DURATION_29).toDouble
        userCallAttribute += -1//s(DURATION_31).toDouble
        userCallAttribute += -1//s(DURATION_28).toDouble


        userNetAttribute += -1//s(NET_FLOW).toDouble
        userNetAttribute += -1//s(REMOTE_NET_DURATION).toDouble
        userNetAttribute += -1//s(LOCAL_NET_DURATION).toDouble
        (userAttribute,userCallAttribute,userNetAttribute)
      }}))
    }).map(x=>(x._1,(x._2._1,x._2._2._1,x._2._2._2,x._2._2._3)))
    val SepUserMessage = SepInterestDimension23.leftOuterJoin(SepUserBasicProfile).map(x=>{
      (x._1,(x._2._1,x._2._2 match {case Some(z) => z;case None => {
        val userAttribute = ArrayBuffer[String]()
        val userCallAttribute = ArrayBuffer[Double]()
        val userNetAttribute = ArrayBuffer[Double]()
        userAttribute += "X"//s(SERV_STATUS)
        userAttribute += "X"//s(WLAN_USE_FLAG)
        userAttribute += "X"//s(CDMA_STATUS)
        userAttribute += "X"//s(OFFER_FLAG)
        userAttribute += "X"//s(EVDO_FLAG)
        userAttribute += "X"//s(FLAG_3G)
        userAttribute += "X"//s(INNET_LENGTH)
        userAttribute += "X"//s(MAIN_OFFER_ID)
        userAttribute += "X"//s(CUST_EDUCATION_LEVEL)
        userAttribute += "X"//s(CUST_GENDER)
        userAttribute += "X"//s(AGE)
        userAttribute += "X"//s(MARRY_STATUS)
        userAttribute += "X"//s(CUST_NATIONALITY)
        userAttribute += "X"//s(BIRTH_DATE)
        userAttribute += "X"//s(MAIN_OFFER_GRADE)
        userAttribute += "X"//s(STAFF_FLAG)
        userAttribute += "X"//s(ARPU_AVERAGE)
        userAttribute += "X"//s(ARPU)

        userCallAttribute += -1//s(DURATION_CALLING).toDouble
        userCallAttribute += -1//s(OUTNET_DURATION_RATE).toDouble
        userCallAttribute += -1//s(CALLING_NUM).toDouble
        userCallAttribute += -1//s(CALLNUM_DROP_RATE).toDouble
        userCallAttribute += -1//s(BILL_CHANGE_RATE).toDouble
        userCallAttribute += -1//s(DURATION_5).toDouble
        userCallAttribute += -1//s(DURATION_17).toDouble
        userCallAttribute += -1//s(DURATION_18).toDouble
        userCallAttribute += -1//s(DURATION_19).toDouble
        userCallAttribute += -1//s(DURATION_16).toDouble
        userCallAttribute += -1//s(DURATION_20).toDouble
        userCallAttribute += -1//s(DURATION_3).toDouble
        userCallAttribute += -1//s(DURATION_1).toDouble
        userCallAttribute += -1//s(DURATION_2).toDouble
        userCallAttribute += -1//s(DURATION_22).toDouble
        userCallAttribute += -1//s(DURATION_23).toDouble
        userCallAttribute += -1//s(DURATION_24).toDouble
        userCallAttribute += -1//s(DURATION_21).toDouble
        userCallAttribute += -1//s(DURATION_25).toDouble
        userCallAttribute += -1//s(DURATION_8).toDouble
        userCallAttribute += -1//s(DURATION_26).toDouble
        userCallAttribute += -1//s(DURATION_27).toDouble
        userCallAttribute += -1//s(DURATION_11).toDouble
        userCallAttribute += -1//s(DURATION_12).toDouble
        userCallAttribute += -1//s(DURATION_14).toDouble
        userCallAttribute += -1//s(DURATION_15).toDouble
        userCallAttribute += -1//s(CALLING_NUM_COUNT).toDouble
        userCallAttribute += -1//s(ALL_CALL_DURATION).toDouble
        userCallAttribute += -1//s(DURATION_4).toDouble
        userCallAttribute += -1//s(DURATION_CALLED).toDouble
        userCallAttribute += -1//s(CALLED_NUM).toDouble
        userCallAttribute += -1//s(DURATION_34).toDouble
        userCallAttribute += -1//s(DURATION_33).toDouble
        userCallAttribute += -1//s(DURATION_32).toDouble
        userCallAttribute += -1//s(DURATION_29).toDouble
        userCallAttribute += -1//s(DURATION_31).toDouble
        userCallAttribute += -1//s(DURATION_28).toDouble


        userNetAttribute += -1//s(NET_FLOW).toDouble
        userNetAttribute += -1//s(REMOTE_NET_DURATION).toDouble
        userNetAttribute += -1//s(LOCAL_NET_DURATION).toDouble
        (userAttribute,userCallAttribute,userNetAttribute)
      }}))
    }).map(x=>(x._1,(x._2._1,x._2._2._1,x._2._2._2,x._2._2._3)))
    val OctUserMessage = OctInterestDimension23.leftOuterJoin(OctUserBasicProfile).map(x=>{
      (x._1,(x._2._1,x._2._2 match {case Some(z) => z;case None => {
        val userAttribute = ArrayBuffer[String]()
        val userCallAttribute = ArrayBuffer[Double]()
        val userNetAttribute = ArrayBuffer[Double]()
        userAttribute += "X"//s(SERV_STATUS)
        userAttribute += "X"//s(WLAN_USE_FLAG)
        userAttribute += "X"//s(CDMA_STATUS)
        userAttribute += "X"//s(OFFER_FLAG)
        userAttribute += "X"//s(EVDO_FLAG)
        userAttribute += "X"//s(FLAG_3G)
        userAttribute += "X"//s(INNET_LENGTH)
        userAttribute += "X"//s(MAIN_OFFER_ID)
        userAttribute += "X"//s(CUST_EDUCATION_LEVEL)
        userAttribute += "X"//s(CUST_GENDER)
        userAttribute += "X"//s(AGE)
        userAttribute += "X"//s(MARRY_STATUS)
        userAttribute += "X"//s(CUST_NATIONALITY)
        userAttribute += "X"//s(BIRTH_DATE)
        userAttribute += "X"//s(MAIN_OFFER_GRADE)
        userAttribute += "X"//s(STAFF_FLAG)
        userAttribute += "X"//s(ARPU_AVERAGE)
        userAttribute += "X"//s(ARPU)

        userCallAttribute += -1//s(DURATION_CALLING).toDouble
        userCallAttribute += -1//s(OUTNET_DURATION_RATE).toDouble
        userCallAttribute += -1//s(CALLING_NUM).toDouble
        userCallAttribute += -1//s(CALLNUM_DROP_RATE).toDouble
        userCallAttribute += -1//s(BILL_CHANGE_RATE).toDouble
        userCallAttribute += -1//s(DURATION_5).toDouble
        userCallAttribute += -1//s(DURATION_17).toDouble
        userCallAttribute += -1//s(DURATION_18).toDouble
        userCallAttribute += -1//s(DURATION_19).toDouble
        userCallAttribute += -1//s(DURATION_16).toDouble
        userCallAttribute += -1//s(DURATION_20).toDouble
        userCallAttribute += -1//s(DURATION_3).toDouble
        userCallAttribute += -1//s(DURATION_1).toDouble
        userCallAttribute += -1//s(DURATION_2).toDouble
        userCallAttribute += -1//s(DURATION_22).toDouble
        userCallAttribute += -1//s(DURATION_23).toDouble
        userCallAttribute += -1//s(DURATION_24).toDouble
        userCallAttribute += -1//s(DURATION_21).toDouble
        userCallAttribute += -1//s(DURATION_25).toDouble
        userCallAttribute += -1//s(DURATION_8).toDouble
        userCallAttribute += -1//s(DURATION_26).toDouble
        userCallAttribute += -1//s(DURATION_27).toDouble
        userCallAttribute += -1//s(DURATION_11).toDouble
        userCallAttribute += -1//s(DURATION_12).toDouble
        userCallAttribute += -1//s(DURATION_14).toDouble
        userCallAttribute += -1//s(DURATION_15).toDouble
        userCallAttribute += -1//s(CALLING_NUM_COUNT).toDouble
        userCallAttribute += -1//s(ALL_CALL_DURATION).toDouble
        userCallAttribute += -1//s(DURATION_4).toDouble
        userCallAttribute += -1//s(DURATION_CALLED).toDouble
        userCallAttribute += -1//s(CALLED_NUM).toDouble
        userCallAttribute += -1//s(DURATION_34).toDouble
        userCallAttribute += -1//s(DURATION_33).toDouble
        userCallAttribute += -1//s(DURATION_32).toDouble
        userCallAttribute += -1//s(DURATION_29).toDouble
        userCallAttribute += -1//s(DURATION_31).toDouble
        userCallAttribute += -1//s(DURATION_28).toDouble


        userNetAttribute += -1//s(NET_FLOW).toDouble
        userNetAttribute += -1//s(REMOTE_NET_DURATION).toDouble
        userNetAttribute += -1//s(LOCAL_NET_DURATION).toDouble
        (userAttribute,userCallAttribute,userNetAttribute)
      }}))
    }).map(x=>(x._1,(x._2._1,x._2._2._1,x._2._2._2,x._2._2._3)))

    val totalUserMessage = JulUserMessage.union(AugUserMessage).union(SepUserMessage).union(OctUserMessage)

    JulUserMessage.map(x=>toString(x)).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/JulUserMessage")
    AugUserMessage.map(x=>toString(x)).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/AugUserMessage")
    SepUserMessage.map(x=>toString(x)).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/SepUserMessage")
    OctUserMessage.map(x=>toString(x)).saveAsTextFile("hdfs://dell01:12306/user/tele/LiuShifeng/OctUserMessage")

    totalUserMessage.map(x=>toString(x)).saveAsTextFile("hdfs://dell01:12306/tele/LiuShifeng/totalUserMessage")


    sc.stop()
  }

  def toString(in:(String,(Array[Double],ArrayBuffer[String],ArrayBuffer[Double],ArrayBuffer[Double]))):String={
    var out = in._1
    val l1 = in._2._1.length
    val l2 = in._2._2.length
    val l3 = in._2._3.length
    val l4 = in._2._4.length
    for(i<-0 until l1){
      out = out + "," + in._2._1(i)
    }
    for(i<-0 until l2){
      out = out + "," + in._2._2(i)
    }
    for(i<-0 until l3){
      out = out + "," + in._2._3(i)
    }
    for(i<-0 until l4){
      out = out + "," + in._2._4(i)
    }
    out
  }
}


