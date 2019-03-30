package practice

import java.text.SimpleDateFormat
import java.util.{Date, GregorianCalendar}
import org.joda.time.{DateTime, Minutes, Seconds}

class getcycleno {

/*    import java.util.Calendar
    val stdate = new GregorianCalendar();
    val eddate = new GregorianCalendar();
    // reset hour, minutes, seconds and millis
    stdate.set(Calendar.HOUR_OF_DAY, 0)
    stdate.set(Calendar.MINUTE, 0)
    stdate.set(Calendar.SECOND, 0)
    stdate.set(Calendar.MILLISECOND, 0)
    println(stdate.getTime)
*/


      val today = new DateTime().withTimeAtStartOfDay();
      val endDate = new DateTime();
      val startDate = endDate.minusMinutes(15)
      //println(today)
      val seconds = Seconds.secondsBetween(today, endDate)
      val minutes = Minutes.minutesBetween(today, endDate)
      //println(seconds)
      //println(minutes)
      val secondsInDay = seconds.getSeconds
      val minutesInday = minutes.getMinutes
     // println(secondsInDay / 900)
      //println(minutesInday / 15)
    val cycle_no = minutesInday / 15


}