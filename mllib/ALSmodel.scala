import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

object ALSmodel {
  def main(args: Array[String]): Unit = {
    // Setting the configuration variable.
    val conf = new SparkConf()
      .setAppName("ALS")
      .setMaster("local");

    val sc = new SparkContext(conf);

    val ratings = sc.textFile("ratings.dat");

    val ratingsData = ratings.map(_.split("::") match {
      case Array(userId, movieId, rating, time) =>
        Rating(userId.toInt, movieId.toInt, rating.toDouble)
    })

     //Split data into training (60%) and test (40%).
    val splits = ratingsData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    
    // Build the recommendation model using ALS
    val rank = 3
    val numIterations = 10
    val model = ALS.train(training, rank, numIterations, 0.01)

    // Evaluate the model on rating data
    val usersProducts = test.map {
      case Rating(userId, movieId, rating) =>
        (userId, movieId)
    }
    val predictions =
      model.predict(usersProducts).map {
        case Rating(userId, movieId, rating) =>
          ((userId, movieId), rating)
      }


    val ratesAndPreds = test.map {
      case Rating(userId, movieId, rating) =>
        ((userId, movieId), rating)
    }.join(predictions)

    //Error

    val MSE = ratesAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()
    
    //Accuracy
    println("Accuracy of ALS model is " + (1-MSE)*100 +" %")
    
  }
}