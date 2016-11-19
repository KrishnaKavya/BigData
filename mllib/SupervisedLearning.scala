import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

object SupervisedLearning {
  def main(args: Array[String]): Unit = {
    // Setting the configuration variable.
    val conf = new SparkConf()
      .setAppName("Supervised Learning")
      .setMaster("local");

    val sc = new SparkContext(conf);

    val data = sc.textFile(args(0));

    //Parsing data. Spliting data based on comma, Considering the class variable and vector  the 
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(10).toDouble, Vectors.dense(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble, parts(3).toDouble, parts(4).toDouble, parts(5).toDouble, parts(6).toDouble, parts(7).toDouble, parts(8).toDouble, parts(9).toDouble))
    }

    //Split data into training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    //Naive Bayes. 
    //Creating model 
    val model = NaiveBayes.train(training, lambda = 1.0)

    //Predicting label for Test data
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))

    //Calculating the accuracy
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count();
    println("Accuracy of Naive Bayes Algorithm is " + accuracy*100 +"%");

    //Decision Tree
    
    val numClasses = 8
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 100

    val DecisionTreeModel = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    val labelAndPreds = test.map { point =>
      val prediction = DecisionTreeModel.predict(point.features)
      (point.label, prediction)
    }
    val DTaccuracy = 100.0 * labelAndPreds.filter(r => r._1 == r._2).count.toDouble / test.count
     println("Accuracy of Decision Tree is " + DTaccuracy + "%")

  }
}