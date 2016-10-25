
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD


object BiGram {
  def main(args: Array[String]): Unit = {

    // Setting the configuration variable.
    val conf = new SparkConf()
      .setAppName("Word Count")
      .setMaster("local")

    val sc = new SparkContext(conf)
    // fetching the File  
    val input = sc.textFile(args(0))
    
    val stopwords = "a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your".split(",")

    var words = input.flatMap(Line => Line.split(" ")).filter(x => !stopwords.contains(x)).map(y => y)

    var map = words.collect().sliding(2).map(x => ((x(0) + " " + x(1), 1))).toSeq

    var result = sc.parallelize(map).reduceByKey((a, b) => a + b).sortBy(_._2).collect().reverse

    val frequencies = args(1).toInt
    println("The bigrams of frequency is above " + frequencies + " are: ")
    
    for (i <- 0 to result.length - 1) {
      if (result(i)._2 >= frequencies) {
        println(result(i))
      }
    }
    
  }
}