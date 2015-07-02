/**
 * Created by LiuShifeng on 2015/5/14.
 */
class Terminal(val data:String) extends Serializable {
  private val monthPosition = 0  //Date
  private val servIdPosition = 1 //Serv_ID
  private val MEIDPosition = 2 //TerminalID
  private val IMSIPosition = 3 //IMSI_IDUIM_ID
  private val brandCodePosition = 4 //BrandCode
  private val brandNamePosition = 5 //BrandName Asic
  private val TerminalCodePosition = 6 //TerminalCode
  private val TerminalNamePosition = 7 //TerminalName Asic
  private val DWPosition = 8 //DoubleWeb Asic
  private val OSPosition = 9 //OS TF
  private val EVDOPosition = 10 //EVDO TF
  private val priceScopePosition = 11 //PriceSlice <400,400-699,700-999,1000-3000,>3000
  private val pricePosition = 12 //RPICE
  private val logTimePosition = 13 //CREATED_DATE calender

  private val s = data.split(",")
  val month = s(monthPosition)
  val servId = s(servIdPosition)
  val brand = if(s(brandCodePosition).equals("")) "U" else s(brandCodePosition)
  val terminal = if(s(TerminalCodePosition).equals("")) "U" else s(TerminalCodePosition)
  /**
  val DW = if(s(DWPosition).equals("T")){
        2
      }else if(s(DWPosition).equals("T")){
        1
      }else 0
    **/
  val OS = if(s(OSPosition).equals("T")) 1 else 0
  val EVDO = if(s(EVDOPosition).equals("T")) 1 else 0
  val price = if(s(pricePosition).equals("")) 0 else s(pricePosition).toInt
  val logTime = new DateUnit((s(logTimePosition)))

  @Override
  override def toString()={
    servId+","+month+","+brand+","+terminal+","+OS+","+EVDO+","+price+","+logTime.toString()
  }

  def toString(s:String)={
    if(s.equals("servId"))
      month+","+brand+","+terminal+","+OS+","+EVDO+","+price+","+logTime.toString()
    else if(s.equals("month"))
      servId+","+brand+","+terminal+","+OS+","+EVDO+","+price+","+logTime.toString()
    else
      servId+","+month+","+brand+","+terminal+","+OS+","+EVDO+","+price+","+logTime.toString()
  }

  def getFeature()={
    (servId,(month,brand,terminal,OS,EVDO,price,logTime.toString()))
  }
}
