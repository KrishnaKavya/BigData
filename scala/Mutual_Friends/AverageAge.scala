import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import java.sql.Date
import java.text.SimpleDateFormat

object AverageAge {
  def main(args: Array[String]): Unit = {
    // Setting the configuration variable.
    val conf = new SparkConf()
      .setAppName("Word Count")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val friendList = sc.textFile(args(0));
    val friends = friendList.map(line => line.split("\\t")).filter(line => (line.size == 2)).map(line => (line(0), line(1).split(","))).flatMap(x => x._2.flatMap(z => Array((z, x._1))))

    // friends.collect().foreach(x => println(x._1+"value= "+x._2))
    
    val userDetails=sc.textFile(args(1))
    val userAge = userDetails.map(line => line.split(",")).map(line => (line(0),((ageDirectFriend(line(9)),line(3),line(4),line(5),line(6),line(7)))))
    
    val userAgeNDetails=friends.join(userAge).map({case(friend,(me,(age,addressLine1,addressLine2,addressLine3,addressLine4,addressLine5)))=>(me,friend, age,addressLine1,addressLine2,addressLine3,addressLine4,addressLine5)})
    
     userAgeNDetails.collect().sortBy(_._3).reverse.take(10).foreach(println);
    //map(case{(friend,(me,age))=>(me,(friend, age))})
    
   // userAgeNDetails.collect().foreach(x=> println(x._1, x._2))
    
  }

  def ageDirectFriend(dob: String) = {
    val todayDate = new Date(System.currentTimeMillis)
    val date = dob.split("/")
    val currentMonth = todayDate.getMonth() + 1
    val currentYear = todayDate.getYear() + 1900
    var sumOfAge = currentYear - date(2).toInt
    if (date(0).toInt > currentMonth) {
      sumOfAge -= 1
    } else if (date(0).toInt == currentMonth) {
      val currentDay = todayDate.getDate();
      if (date(1).toInt > currentDay) {
        sumOfAge -= 1
      }
    }
    sumOfAge.toFloat
  }

}