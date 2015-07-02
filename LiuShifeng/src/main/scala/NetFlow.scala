/**
 * Created by LiuShifeng on 2015/5/15.
 */
class NetFlow(val data:String) extends Serializable {
  private val monthPosition = 0
  private val servIDPosition = 1
  private val startTimePosition = 4
  private val endTimePosition = 5
  private val durationPosition = 6
  private val byteOutPostion = 7
  private val byteInPosition = 8

  private val s = data.split(",")

  val month = s(monthPosition)
  val servId = s(servIDPosition)
  val startTime = new DateUnit(s(startTimePosition))
  val endTime = new DateUnit(s(endTimePosition))
  val duration = s(durationPosition).toInt
  val byteOut = s(byteOutPostion).toInt
  val byteIn = s(byteInPosition).toInt
  val flow = byteOut+byteIn

  @Override
  override def toString():String = {
    servId+","+month+","+startTime.toString()+","+endTime.toString()+","+duration+","+byteOut+","+byteIn
  }

  def toString(x:String):String = {
    if(x.equals("servId"))
      month+","+startTime.toString()+","+endTime.toString()+","+duration+","+byteOut+","+byteIn
    else if(x.equals("month"))
      servId+","+startTime.toString()+","+endTime.toString()+","+duration+","+byteOut+","+byteIn
    else
      servId+","+month+","+startTime.toString()+","+endTime.toString()+","+duration+","+byteOut+","+byteIn
  }

  def getFeature()={
    (servId,(flow,byteOut,byteIn,startTime,endTime,duration))
  }
}
