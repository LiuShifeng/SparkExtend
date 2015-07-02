/**
 * Created by LiuShifeng on 2015/4/28.
 */
import org.apache.spark.rdd._

class UserMarketProfile(val file:RDD[String]) extends Serializable{
  private val acctMonthPosition = 0 //Month Numeric
  private val servIDPosition = 1 //ServID Numeric
  private val productOfferInstanceIDPostion = 4 //productInstance  Numeric
  private val productOfferIDPosition = 6 //productId Numeric
  private val expDatePosition = 9 //endDate TimeStamp
  private val effDatePosition = 10 //beginDate TimeStamp
  private val offerCompletedDatePosition = 11 //completeDate TimeStamp

  var id = ""
  var productOfferInstance = ""
  var productOfferId = ""
  var expDate = ""
  var effDate = ""

  @Override
  override def toString():String={
    id+","+productOfferInstance+","+productOfferId+","+expDate+","+effDate
  }

  def getUserProfile()= {
    val MarketingMap = file.map(x => {
      val s = x.split(",")
      id = s(servIDPosition)
      productOfferInstance = s(productOfferInstanceIDPostion)
      productOfferId = s(productOfferIDPosition)
      expDate = s(expDatePosition)
      effDate = s(effDatePosition)
      (id, (productOfferInstance, productOfferId, expDate, effDate))
    })
  }

}
