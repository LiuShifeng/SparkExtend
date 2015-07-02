import org.apache.spark.rdd.RDD

/**
 * Created by LiuShifeng on 2015/5/6.
 */
class UserBasicProfile(val file:RDD[String]) extends Serializable {
    def getUserBasicProfile()={
        val BasicProfile = file.map(x=>{
            val servIdProfile = new ServIdProfile(x)
            (servIdProfile.Id,(servIdProfile.userAttribute,servIdProfile.userCallAttribute,servIdProfile.userNetAttribute))
        })
        BasicProfile
    }
}
