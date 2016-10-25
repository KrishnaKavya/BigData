
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

object InMemoryJoin {
  def main(args: Array[String]): Unit = {

    // Setting the configuration variable.
    val conf = new SparkConf()
      .setAppName("Word Count")
      .setMaster("local")

    val sc = new SparkContext(conf)

    // Fetching the the friend1 and friend2 id's 
    val friend1 = args(0);
    val friend2 = args(1);
   
    //fetching the user data and friends List File
    val userData = sc.textFile(args(2))
    val friendsFile = sc.textFile(args(3));
   
    //emiting a map of user number as key and name and date of birth as value. for all the lines in the file.
    var friendInfo = userData.flatMap(_.split("\n")).map(_.split(",")).map(x => (x(0), (x(1), x(9))))
    
    //Finding mutual friends. Finding the mutual friends. (refer to mutualfriends.scala.
    val map = friendsFile.flatMap(_.split("\n")).map(_.split("\t")).map(x => (x(0), x(1)))
    var friendsList1 = map.filter(x => x._1.equals(friend1)).map(x => (x._2)).first().split(",")
    var friendsList2 = map.filter(x => x._1.equals(friend2)).map(x => (x._2)).first().split(",")
    val result = friendsList1.intersect(friendsList2).map(x => (x, x))
    var res = sc.parallelize(result)

    //Joining the result of mutual friends list to the user data. 
    var data = res.join(friendInfo)
    
    //printing the data.
    data.foreach(x=>println(x._2))
    

  }

}