import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object Kmeans {
  def main(args: Array[String]): Unit = {

    // Setting the configuration variable.
    val conf = new SparkConf()
      .setAppName("K Means")
      .setMaster("local");

    val sc = new SparkContext(conf);
    //Fetching data.
    var ratings = sc.textFile(args(0));
    var moviesFile = sc.textFile(args(1));

    //Converting the rating matrix to map of movie id, ratings of each user. 
    val parsedData = ratings.map(s => Vectors.dense(s.split(' ').drop(1).map(_.toDouble))).cache()

    //emit movie id as key,  name and genre as value. 
    var moviesData = moviesFile.map(line => line.split("::")).map(s => (s(0), (s(1) + "," + s(2))));

    //setting the number of clusters and iterations.
    val numberOfClusters = 10;
    val numberOfIterations = 10;

    //Clustering using K Means 
    var clusters = KMeans.train(parsedData, numberOfClusters, numberOfIterations);

    //Predicting output for each line and emiting the key as movie and value as predicted cluster id.
    val predict = ratings.map(line => (line.split(' ')(0), clusters.predict(Vectors.dense(line.split(' ').drop(1).map(_.toDouble)))));

    //joining the predicted value to the movies data. join is done based on key i.e, movie id. 
    val joinData = predict.join(moviesData);
    
    //grouping based on Cluster id.
    val groupByCluster = joinData.map(s => (s._2._1, (s._1, s._2._2))).groupByKey();
    
    //Converting to list in order to pick 5 elements.
    val intermediate=groupByCluster.map(p=> (p._1,p._2.toList));
    val result=intermediate.map(p=> (p._1, p._2.take(5)));
    
    // Printing Output
    result.collect().foreach(Line => println(Line._1," "+Line._2) );
 
    
    
    
  }
}