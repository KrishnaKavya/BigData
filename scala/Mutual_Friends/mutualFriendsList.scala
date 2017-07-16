
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

object mutualFriendsList {
  def main(args: Array[String]): Unit = {
    
     // Setting the configuration variable.
    val conf = new SparkConf()
      .setAppName("Mutual Friends List")
      .setMaster("local")
      
    val sc = new SparkContext(conf)
    
    //reading input file.
    val friendsFile = sc.textFile(args(0));
    val lines = friendsFile.flatMap(_.split("\n")).map(_.split("\t")).filter(x =>x.length>1).map(x => (x(0), x(1)))
  //  lines.collect().foreach(x=>println(x._1+"  value= "+x._2))
    
    var friendsPairs=lines.flatMap(x=>(x._2.split(",").map(y=>(y,x._1))))
    friendsPairs.collect().foreach(x=>extract(x._1,x._2,lines))
    
  }
  
  def  extract(friend1:String,friend2:String, lines:RDD[(String,String)]) ={
   
    var friendsList1 = lines.filter(x => x._1.equals(friend1)).map(x => (x._2)).first().split(",")
    var friendsList2 = lines.filter(x => x._1.equals(friend2)).map(x => (x._2)).first().split(",")
    val result = friendsList1.intersect(friendsList2).map(x => ( x))
    println("common friends for "+friend1+ " : "+ friend2);
    result.foreach(println)
  }
}
