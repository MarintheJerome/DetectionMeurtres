import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession

/**
  * An example for Multilayer Perceptron Classification.
  */
object TestMutilayerPerceptron {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("testx").setMaster("local[*]")
    // Création du context spark
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .appName("MultilayerPerceptronClassifierExample")
      .getOrCreate()

    import spark.implicits._

    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm")
      .load("Category.txt")

    // Split the data into train and test
    val splits = data.randomSplit(Array(0.7, 0.3))
    val train = splits(0)
    val test = splits(1)

    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](21, 17, 14)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setMaxIter(5)

    // train the model
    val model = trainer.fit(train)

    // compute accuracy on the test set
    val result = model.transform(test)
    val predictionAndLabels = result.select("prediction", "label")
    val predAndLabel = predictionAndLabels.rdd.map(row => (row.getDouble(0), row.getDouble(1)))
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    val acc = evaluator.evaluate(predictionAndLabels)

    println(predAndLabel)
    val metrics = new MulticlassMetrics(predAndLabel)


    println("Test set accuracy = " + acc)
    println("Matrix de confusion : \n" + metrics.confusionMatrix)
    println("Nb de poids : " + model.weights.size)
    println("Poids : " + model.weights)



    spark.stop()
  }
}