/**
 * Created by LiuShifeng on 2015/5/11.
 */

import scala.collection.mutable._

class ServIdProfile(val data:String) extends Serializable {
  private val SERV_ID = 2//	用户ID String
  //    val LATN_ID = 4//	本地网标识 String
  private val SIM_TYPE = 7//	SIM卡类型 String
  private val MHLEVEL = 8//	中高端标识 String
  //    val INCREMENT_FLAG = 15//	增存量标识 TF
  private val SERV_STATUS = 9//	在网用户标识 TF
  private val OWE_BUSINESS_TYPE_ID = 10//	用户欠费状态(虚增标识) String
  private val CALL_FLAG = 12//	通话用户标识 String
  private val ACTIVE_FLAG = 13 //	活跃用户标识 String
  //    val MHLEVEL_ADD_FLAG = 25	//当月新增中高端标识 TF
  //    val SILENCE_SERV = 27	//沉默用户标识 TF
  private val WLAN_USE_FLAG = 18//	WLAN用户标识 TF
  private val CDMA_STATUS = 19//	C网用户在网标识 TF
  private val ONLINE_FLAG = 20//	在网标志 String
  private val MHLEVEL_LOST = 21 //	移动中高端流失保有标志 String
  private val OFFER_FLAG = 22//	套餐标志 String
  private val COMP_SERV_FLAG = 23//	融合业务标志 String
  private val EVDO_FLAG = 24//	EVDO标志 String
  private val FLAG_3G = 25//	3G标志 String
  private val INNET_LENGTH = 26//	在网时长 String
  private val STATE_DATE = 27//	状态时间 Date
  private val STATE = 28//	状态 String
  private val BC_STATE_CODE = 29//	用户状态(基础编码) String
  //    val CREDIT_1_EXP_DATE = 52//	初始/普通信用度失效时间 Date
  //    val CREDIT_1_EFF_DATE = 53//	初始/普通信用度生效时间 Date
  //    val ACCT_FINAN_BALANCE = 54//	账号余额（包括专款+信控） Double
  //    val SERV_FINAN_BALANCE = 55//	用户余额（包括专款+信控） Double
  //    val CREDIT_2 = 56//	特殊信用额度 Double
  //    val CREDIT_1 = 57//	普通信用额度 Double
  //    val CREDIT_3 = 58//	临时信用额度 Double
  private val MAIN_OFFER_ID = 31//	主流套餐id String
  private val MAIN_OFFER_SUBCLASS = 32//	主流套餐细类 String+ASIC
  private val MAIN_OFFER_CLASSIFY = 33//	主流套餐分类 ASIC
  private val EXTEND_ATTR = 34//	存量附加属性 ASIC
  private val IMPORTANCE_LEVEL = 38//	客户重要等级(红灰黑名单)String
  private val CUST_EDUCATION_LEVEL = 39//	客户学历 String
  private val CUST_GENDER = 40//	客户性别 TF
  private val CUST_BRAND = 41//	客户品牌 String
  private val AGE = 42//	客户年龄 Int
  private val MARRY_STATUS = 43//	客户婚姻状况 Int
  private val CUST_NATIONALITY = 44//	客户国籍/民族 String
  private val SERVICE_LEVEL = 45//	客户服务等级 Int
  private val BIRTH_DATE = 46//	客户的生日 Date
  private val MAIN_OFFER_GRADE = 54//天翼套餐档次
  private val IVPN_FLAG = 65//	是否总机服务用户 TF
  private val STAFF_FLAG = 66//	是否员工 TF
  //    val DURATION_MAX = 118	//最长单次通话时长(账期内) Int
  //    val RATE_DURATION_MAX = 119//	最长单次计费时长(账期内) Int
  //    val OWE_STOP_NUM = 126//	最近3个月用户欠费停机次数 Int
  //    val COMPLAIN_NUM_3 = 127//	最近3个月投诉次数 Int
  private val ARPU_AVERAGE = 74//	最近3个月平均ARPU Double
  //    val OWE_MONTH_NUM = 128	//最近3个月内欠费记录次数 Int
  //    val NOTICE_NUM_3 = 129//最近3个月内短信催缴次数 Int
  //    val COMPLAIN_NUM_2 = 130//	最近2个月投诉次数 Int
  //    val NOTICE_NUM_2 = 131//	最近2个月内短信催缴次数 Int
  //    val COMPLAIN_NUM_1 = 132//	最近1个月投诉次数 Int
  //    val NOTICE_NUM_1 = 133//	最近1个月内短信催缴次数 Int
  private val DURATION_CALLING = 75//	主叫通话时长(月汇总) second Double
  private val OUTNET_DURATION_RATE = 76//	主叫其他运营商时长比例 Doule
  private val CALLING_NUM = 78//	主叫次数(月汇总) Int
  private val CALLNUM_DROP_RATE = 80//	用户月通话次数突减百分比 Double
  private val BILL_CHANGE_RATE = 81//	用户话费连续2个月突减/突增百分比 Double
  private val ARPU = 82//	用户ARPU值 Double
  private val DURATION_5 = 91//	移动本地通话时长 Double
  private val REMOTE_NET_DURATION = 98//	省外上网时长（小时）Double
  private val LOCAL_NET_DURATION = 99//	省内上网时长（小时）Double
  private val DURATION_17 = 101//	省内漫游国内长途通话(呼叫非漫游)时长 Double
  private val DURATION_18 = 103//	省内漫游国际长途通话时长Double
  private val DURATION_19 = 106//	省内漫游港澳台长途通话时长Double
  private val DURATION_16 = 110//	省内漫游本地通话(呼叫漫游地)时长 Double
  private val DURATION_20	= 112//省内漫游被叫通话时长 Double
  private val DURATION_3 = 115//	上网通信时长 Double
  private val NET_FLOW = 118//	上网流量（MB） Double
  private val DURATION_1 = 119//	区内通话时长 Double
  private val DURATION_2 = 122//区间通话时长 Double
  //    val AVG_ROA_DURATION = 205//	前3个月平均漫游时长Double
  //    val AVG_NAT_DURATION = 207//	前3个月平均国内长话时长 Double
  //    val AVG_ABD_DURATION = 209	//前3个月平均国际长话时长 Double
  private val DURATION_22	= 133//国内漫游国内长途通话(呼叫非漫游)时长Double
  private val DURATION_23 = 135//	国内漫游国际长途通话时长 Double
  private val DURATION_24 = 138//	国内漫游港澳台长途通话时长 Double
  private val DURATION_21 = 142 //	国内漫游本地通话(呼叫漫游地)时长 Double
  private val DURATION_25 = 144//	国内漫游被叫通话时长 Double
  private val DURATION_8 = 147//	国内传统长话时长 Double
  private val DURATION_26 = 150//	国际漫游主叫通话时长 Double
  private val DURATION_27 = 153//	国际漫游被叫通话时长 Double
  private val DURATION_11 = 156//	国际传统长途通话时长 Double
  private val DURATION_12 = 159//	国际IP长途通话时长Double
  private val DURATION_14 = 162//	港澳台传统长途时长 Double
  private val DURATION_15 = 165//	港澳台IP长途时长 Double
  private val CALLING_NUM_COUNT = 170//	本月拨打号码个数 Double
  private val ALL_CALL_DURATION = 171//	本地及国内长途通话时长（分钟） Double
  private val DURATION_4 = 172//	本地被叫通话时长 Double
  private val DURATION_CALLED = 173//	被叫通话时长(月汇总) Double
  private val CALLED_NUM = 177//被叫次数(月汇总) Int
  //    val DURATION_9 = 290//	IP国内长途时长 Double
  //    val DURATION_7 = 294//	C网语音其他时长 Double
  //    val DURATION_36 = 303//	(1403)国内漫游主叫通话时长 Double
  //    val DURATION_35 = 307//	(1401)省内漫游主叫通话时长 Double
  //    val DURATION_30 = 310	//(14)漫游状态通话时长 Double
  private val DURATION_34 = 209//	(0203)港澳台长途通话时长 Double
  private val DURATION_33 = 212//	(0202)国际长途通话时长 Double
  private val DURATION_32 = 215//	(0201)国内长途通话时长 Double
  private val DURATION_29 = 218//	(02)长途通信通话时长 Double
  private val DURATION_31 = 221//	(0102)本地通话时长 Double
  private val DURATION_28 = 224//	(01)本地通信通话时长 Double

  val s = data.split(",")

  val Id = s(SERV_ID)
  val userAttribute = ArrayBuffer[String]()
  val userCallAttribute = ArrayBuffer[Double]()
  val userNetAttribute = ArrayBuffer[Double]()

  userAttribute += s(SERV_STATUS)
  userAttribute += s(WLAN_USE_FLAG)
  userAttribute += s(CDMA_STATUS)
  userAttribute += s(OFFER_FLAG)
  userAttribute += s(EVDO_FLAG)
  userAttribute += s(FLAG_3G)
  userAttribute += s(INNET_LENGTH)
  userAttribute += s(MAIN_OFFER_ID)
  userAttribute += s(CUST_EDUCATION_LEVEL)
  userAttribute += s(CUST_GENDER)
  userAttribute += s(AGE)
  userAttribute += s(MARRY_STATUS)
  userAttribute += s(CUST_NATIONALITY)
  userAttribute += s(BIRTH_DATE)
  userAttribute += s(MAIN_OFFER_GRADE)
  userAttribute += s(STAFF_FLAG)
  userAttribute += s(ARPU_AVERAGE)
  userAttribute += s(ARPU)

  userCallAttribute += s(DURATION_CALLING).toDouble
  userCallAttribute += s(OUTNET_DURATION_RATE).toDouble
  userCallAttribute += s(CALLING_NUM).toDouble
  userCallAttribute += s(CALLNUM_DROP_RATE).toDouble
  userCallAttribute += s(BILL_CHANGE_RATE).toDouble
  userCallAttribute += s(DURATION_5).toDouble
  userCallAttribute += s(DURATION_17).toDouble
  userCallAttribute += s(DURATION_18).toDouble
  userCallAttribute += s(DURATION_19).toDouble
  userCallAttribute += s(DURATION_16).toDouble
  userCallAttribute += s(DURATION_20).toDouble
  userCallAttribute += s(DURATION_3).toDouble
  userCallAttribute += s(DURATION_1).toDouble
  userCallAttribute += s(DURATION_2).toDouble
  userCallAttribute += s(DURATION_22).toDouble
  userCallAttribute += s(DURATION_23).toDouble
  userCallAttribute += s(DURATION_24).toDouble
  userCallAttribute += s(DURATION_21).toDouble
  userCallAttribute += s(DURATION_25).toDouble
  userCallAttribute += s(DURATION_8).toDouble
  userCallAttribute += s(DURATION_26).toDouble
  userCallAttribute += s(DURATION_27).toDouble
  userCallAttribute += s(DURATION_11).toDouble
  userCallAttribute += s(DURATION_12).toDouble
  userCallAttribute += s(DURATION_14).toDouble
  userCallAttribute += s(DURATION_15).toDouble
  userCallAttribute += s(CALLING_NUM_COUNT).toDouble
  userCallAttribute += s(ALL_CALL_DURATION).toDouble
  userCallAttribute += s(DURATION_4).toDouble
  userCallAttribute += s(DURATION_CALLED).toDouble
  userCallAttribute += s(CALLED_NUM).toDouble
  userCallAttribute += s(DURATION_34).toDouble
  userCallAttribute += s(DURATION_33).toDouble
  userCallAttribute += s(DURATION_32).toDouble
  userCallAttribute += s(DURATION_29).toDouble
  userCallAttribute += s(DURATION_31).toDouble
  userCallAttribute += s(DURATION_28).toDouble

  userNetAttribute += s(NET_FLOW).toDouble
  //userNetAttribute += s(REMOTE_NET_DURATION).toDouble
  //userNetAttribute += s(LOCAL_NET_DURATION).toDouble

  def userAttributeToString():String = {
    val out = userAttribute.mkString(",")
    out
  }
  def userCallAttributeToString():String = {
    var out = userCallAttribute.mkString(",")
    out
  }
  def userNetAttributeToString():String = {
    val l = userNetAttribute.length
    var out = userNetAttribute(0).toString
    out
  }

  @Override
  override def toString():String = {
    Id+","+userAttributeToString()+","+userCallAttributeToString()+","+userNetAttributeToString()
  }
}
