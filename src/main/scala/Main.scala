import scala.slick.driver.PostgresDriver.simple._

object Main {

  case class Header(id: String, year: String, month: String, day: String, hour: String, reltime: String, numlev: String, p_src: String, np_src: String, lat: String, lon: String)

  case class Data(lvltype1: String, lvltype2: String, etime: String, press: String, pflag: String, gph: String, zflag: String, temp: String, tflag: String, rh: String, dpdp: String, wdir: String, wspd: String)

  case class SoundingRow(header: Header, data: Data)

  class Sounding(tag: Tag) extends Table[SoundingRow](tag, "Sounding") {

    def id = column[String]("ID")
    def year = column[String]("YEAR")
    def month = column[String]("MONTH")
    def day = column[String]("DAY")
    def hour = column[String]("HOUR")
    def reltime = column[String]("RELTIME")
    def numlev = column[String]("NUMLEV")
    def p_src = column[String]("P_SRC")
    def np_src = column[String]("NP_SRC")
    def lat = column[String]("LAT")
    def lon = column[String]("LON")
    def lvltype1 = column[String]("LVLTYPE1")
    def lvltype2 = column[String]("LVLTYPE2")
    def etime = column[String]("ETIME")
    def press = column[String]("PRESS")
    def pflag = column[String]("PFLAG")
    def gph = column[String]("GPH")
    def zflag = column[String]("ZFLAG")
    def temp = column[String]("TEMP")
    def tflag = column[String]("TFLAG")
    def rh = column[String]("RH")
    def dpdp = column[String]("DPDP")
    def wdir = column[String]("WDIR")
    def wspd = column[String]("WSPD")

    private type HeaderTupleType = (String, String, String, String, String, String, String, String, String, String, String)
    private type DataTupleType = (String, String, String, String, String, String, String, String, String, String, String, String, String)
    private type SoundingRowTupleType = (HeaderTupleType, DataTupleType)

    private val soundingShapedValue = (
      (id, year, month, day, hour, reltime, numlev, p_src, np_src, lat, lon),
      (lvltype1, lvltype2, etime, press, pflag, gph, zflag, temp, tflag, rh, dpdp, wdir, wspd)).shaped[SoundingRowTupleType]

    private val toSoundingRow: (SoundingRowTupleType => SoundingRow) = { soundingTuple =>
      SoundingRow(header = Header.tupled.apply(soundingTuple._1), data = Data.tupled.apply(soundingTuple._2))
    }

    private val toSoundingTuple: (SoundingRow => Option[SoundingRowTupleType]) = { soundingRow =>
      Some(Header.unapply(soundingRow.header).get, Data.unapply(soundingRow.data).get)
    }

    def * = soundingShapedValue <> (toSoundingRow, toSoundingTuple)
  }

  def main(args: Array[String]): Unit = {
    val connectionUrl = "jdbc:postgresql://localhost/mydb?user=hadoop&password=hadoop"

    Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
      implicit session =>
        val sounding = TableQuery[Sounding]
        sounding.ddl.create
    }

    val filename = "/home/hadoop/rb/data/USM00070219-data.txt"
    insertData(connectionUrl, filename)

  }

  def insertData(connectionUrl: String, filename: String): Unit = {
    import scala.io.Source

    var id = ""
    var year = ""
    var month = ""
    var day = ""
    var hour = ""
    var reltime = ""
    var numlev = ""
    var p_src = ""
    var np_src = ""
    var lat = ""
    var lon = ""
    for (line <- Source.fromFile(filename).getLines) {
      if (line.slice(0, 1).equals("#")) {
        id = line.slice(1, 12)
        year = line.slice(13, 17)
        month = line.slice(18, 20)
        day = line.slice(21, 23)
        hour = line.slice(24, 26)
        reltime = line.slice(27, 31)
        numlev = line.slice(32, 36)
        p_src = line.slice(37, 45)
        np_src = line.slice(46, 54)
        lat = line.slice(55, 62)
        lon = line.slice(63, 71)
      } else {
        val lvltype1 = line.slice(0, 1)
        val lvltype2 = line.slice(1, 2)
        val etime = line.slice(3, 8)
        val press = line.slice(9, 15)
        val pflag = line.slice(15, 16)
        val gph = line.slice(16, 21)
        val zflag = line.slice(21, 22)
        val temp = line.slice(22, 27)
        val tflag = line.slice(27, 28)
        val rh = line.slice(28, 33)
        val dpdp = line.slice(34, 39)
        val wdir = line.slice(40, 45)
        val wspd = line.slice(46, 51)
        Database.forURL(connectionUrl, driver = "org.postgresql.Driver") withSession {
          implicit session =>
            val sounding = TableQuery[Sounding]
            val header = Header(id, year, month, day, hour, reltime, numlev, p_src, np_src, lat, lon)
            val data = Data(lvltype1, lvltype2, etime, press, pflag, gph, zflag, temp, tflag, rh, dpdp, wdir, wspd)
            val soundingRow = SoundingRow(header, data)
            sounding += (soundingRow)
        }
      }
    }

  }
}
