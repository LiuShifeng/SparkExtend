/**
 * Created by LiuShifeng on 2015/1/18.
 */
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

class DateUnit(){
  var mDateTimeStringFormat = "yyyy-MM-dd HH:mm:ss"
  val cald = Calendar.getInstance()
  var mDateStringFormat = "yyyy-MM-dd"
  var mTimeStringFormat = "HH:mm:ss"
  val ONE_DAY_MILLISECONDS:Long = 86400000

  /**
   * to String
   */
  override def toString():String ={
    val out = getYear()+","+getMonth()+","+getDay()+","+getHour()+","+getMinute()+","+getSecond()
    out
  }

  /**
   * timestamp as input
   * @param time:Long
   */
  def this(time:Long){
    this()
    setTime(time)
  }

  /**
   *timestamp as input
   * @param dt:String
   */
  def this(dt:String){
    this()
    setTime(dt)
  }

  /**
   *specific time as input
   * @param year:Int
   * @param month:Int
   * @param day;Int
   * @param hour:Int
   * @param minute:Int
   * @param second:Int
   */
  def this(year:Int,month:Int,day:Int,hour:Int,minute:Int,second:Int){
    this()
    setTime(year, month, day, hour,minute,second)
  }

  /**
   * set time
   * timestamp as input
   * @param time:Long
   */
  def setTime(time:Long)={
    val datetime = new Date(time)
    cald.setTime(datetime)
  }

  /**
   * set time
   * timestamp as input
   * @param dt;String
   */
  def setTime(dt:String)={
    val mDateFormat = new SimpleDateFormat(mDateTimeStringFormat)
    val datetime = mDateFormat.parse(dt)
    cald.setTime(datetime)
  }

  /**
   * set time
   * specific time as input
   * @param year:Int
   * @param month:Int
   * @param day;Int
   * @param hour:Int
   * @param minute:Int
   * @param second:Int
   */
  def setTime(year:Int,month:Int,day:Int,hour:Int,minute:Int,second:Int)={
    cald.set(year, month, day, hour, minute, second)
  }

  /**
   * set year,month,dat
   * specific time as input
   * @param year:Int
   * @param month;Int
   * @param day:Int
   */
  def setDateTime(year:Int,month:Int,day:Int)={
    cald.set(year, month, day)
  }

  /**
   * set hour,minute,second
   * specific time as input
   * @param hour:Int
   * @param minute:Int
   * @param second:Int
   */
  def setTimeTime(hour:Int,minute:Int,second:Int)={
    cald.set(Calendar.HOUR_OF_DAY, hour)
    cald.set(Calendar.MINUTE, minute)
    cald.set(Calendar.SECOND, second)
  }

  /**
   * set hour,minute
   * specifi time as input
   * @param hour:Int
   * @param minute:Int
   */
  def setShortTimeTime(hour:Int, minute:Int)={
    cald.set(Calendar.HOUR, hour)
    cald.set(Calendar.MINUTE, minute)
  }

  /**
   * get date in String
   * yyyy-MM-dd
   * @return
   */
  def getDateString():String = {
    val mDateFormat = new SimpleDateFormat(mDateStringFormat)
    mDateFormat.format(cald.getTime())
  }

  /**
   * get time in String
   * HH:mm:ss
   * @return
   */
  def getTimeString():String={
    val mDateFormat = new SimpleDateFormat(mTimeStringFormat)
    mDateFormat.format(cald.getTime())
  }

  /**
   * get time in millsecond
   *
   * @return Long
   */
  def getMillsecond():Long={
    cald.getTime().getTime()
  }

  /**
   * get year
   * @return Int
   */
  def getYear():Int={
    cald.get(Calendar.YEAR)
  }

  /**
   * get month(1-12)
   * @return Int
   */
  def getMonth():Int={
    cald.get(Calendar.MONTH)
  }

  /**
   * get day of month(start from 1)
   * @return Int
   */
  def getDay():Int={
    cald.get(Calendar.DAY_OF_MONTH)
  }

  /**
   * get day of week(0-Sun,1-Mon,...,6-Sat)
   * @return Int
   */
  def getWeek():Int={
    cald.get(Calendar.DAY_OF_WEEK)
  }

  /**
   *get hour of day(0-23)
   * @return Int
   */
  def getHour():Int={
    cald.get(Calendar.HOUR_OF_DAY)
  }

  /**
   * get minute
   * @return Int
   */
  def getMinute():Int={
    cald.get(Calendar.MINUTE)
  }

  /**
   * get second
   * @return Int
   */
  def getSecond():Int={
    cald.get(Calendar.SECOND)
  }

  /**
   * set DateStringFormat
   * @param dsf:String
   */
  def setDateStringFormat(dsf:String)={
    mDateStringFormat = dsf
  }

  /**
   * set TimeStringFormat
   * @param tsf:String
   */
  def setTimeStringFormat(tsf:String)={
    mTimeStringFormat = tsf
  }

  /**
   * set DateTimeStringFormat
   * @param tsf:String
   */
  def setDateTimeStringFormat(tsf:String)={
    mDateTimeStringFormat = tsf
  }

  /**
   * get the start of the data in millsecond
   * @return Long
   */
  def getDayStartTick():Long={
    setTimeTime(0,0,0)
    ((getMillsecond()/1000.0).toLong)*1000
  }

  /**
   * get the end of the data in millsecond
   * @return Long
   */
  def getDayEndTick():Long={
    getDayStartTick() + ONE_DAY_MILLISECONDS
  }

  /**
   * how many days have the person been alive
   * @param birthday:Long
   * @return ;Double
   */
  def getLiveDay(birthday:Long):Double={
    val curr = System.currentTimeMillis()
    val days = (curr - birthday)/(24*60*60*1000).toDouble
    days
  }
}
