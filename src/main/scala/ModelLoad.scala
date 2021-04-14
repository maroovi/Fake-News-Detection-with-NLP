import Data.create_dataframe
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

object ModelLoad extends App {

  val session = SparkSession.builder()
    .master("local")
    .appName("Load Model")
    .getOrCreate()

  val model = PipelineModel.read.load("src/test/scala/resources/model/myRandomForestClassificationModels")

  val predictions = model.transform(create_dataframe())

  //predictions.show(10)

  val evaluater = new MulticlassClassificationEvaluator().setLabelCol("target").setPredictionCol("fake_predict").setMetricName("accuracy")

  val accuracy = evaluater.evaluate(predictions)

  println(accuracy)

}
