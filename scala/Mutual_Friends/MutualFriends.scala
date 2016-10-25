import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
object MutualFriends {
  def main(args: Array[String]) {
    // Setting the configuration variable.
    val conf = new SparkConf()
      .setAppName("Word Count")
      .setMaster("local")

    val sc = new SparkContext(conf)

    // Fetching the the friend1 and friend2 id's 
    val friend1 = args(0);
    val friend2 = args(1);

    // reading an input file. 
    val friendsFile = sc.textFile("soc-LiveJournal1Adj.txt");
    /*
     * taking the file and splitting each line and for each line we split it by 
     * tab to separate user id and the friendsList of id's.
     * The map is an rdd of type [(String,String)]
     * 
     */
    val map = friendsFile.flatMap(_.split("\n")).map(_.split("\t")).map(x => (x(0), x(1)))

    /*
     * The filter method searches for each line if the the user id(first element of map) is equal to friend1.
     * The second part of array has the String of all friends separated by coma. Hence, we split the output of filter with , 
     * and take it in a Array of String.   
     */
    var friendsList1 = map.filter(x => x._1.equals(friend1)).map(x => (x._2)).first().split(",")

    /*
     *  Filter for friend2 gives the array of friends of friend2 
     */
    var friendsList2 = map.filter(x => x._1.equals(friend2)).map(x => (x._2)).first().split(",")

     /*
      * Intersection of friendsList 1, friendsList2 gives the mutual friends.
      */
    val result = friendsList1.intersect(friendsList2)

    result.foreach(println)

  

  }
}