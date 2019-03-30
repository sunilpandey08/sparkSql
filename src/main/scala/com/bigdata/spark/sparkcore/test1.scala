import com.bigdata.spark.sparkcore.singletonObject
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

import org.joda.time.{DateTime, Minutes, Seconds}
import java.text.SimpleDateFormat

object test1 {
  def main(args: Array[String]) {

    val today = new DateTime().withTimeAtStartOfDay();
    val endDate = new DateTime();
    val startDate = endDate.minusMinutes(15)
    val cycleno = startDate.getHourOfDay + ":" + startDate.getMinuteOfHour + "-" + endDate.getHourOfDay + ":" + endDate.getMinuteOfHour
    println(cycleno.toString)
    //println(today)
    val v  = (endDate.hourOfDay()+":"+endDate.minuteOfHour() + "-" + startDate.hourOfDay()+":"+startDate.minuteOfHour())
    println(v)
    val seconds = Seconds.secondsBetween(today, endDate)
    val minutes = Minutes.minutesBetween(today, endDate)
    //println(seconds)
    //println(minutes)
    val secondsInDay = seconds.getSeconds
    val minutesInday = minutes.getMinutes
    // println(secondsInDay / 900)
    //println(minutesInday / 15)
    val cycle_no = minutesInday / 15
    val format = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss")
   //println(format.format(DateTime().toString())+"-"+format.format((DateTime().minusMinutes(15)).toString()))
    //val format = new SimpleDateFormat("YYYYMMddhhmmss")
    import org.apache.spark.sql.functions.expr
    import java.sql.Timestamp
    var cal = Calendar.getInstance()
  //println((cal.getTime)- expr("INTERVAL 15 MINUTES"))

    //val dateofnow = format.format((cal.getTime)).toString()

      }

    }
