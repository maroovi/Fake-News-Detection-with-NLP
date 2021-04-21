import Data.preprocess_data
import TFIDF.{pipeline_stages, tf_idf}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object ModelLoad extends App {

  val session = SparkSession.builder()
    .master("local")
    .appName("Load Model")
    .getOrCreate()

  session.sparkContext.setLogLevel("ERROR")

  val test = session.read
    .options(Map("header" -> "true", "quote" -> "\"", "escape" -> "\""))
    .csv("src/test/scala/resources/datasets/test.csv")

  //val model = PipelineModel.read.load("src/test/scala/resources/model/myRandomForestClassificationModels")

  //  val predictions = model.transform(create_dataframe())
  //
  //  //predictions.show(10)
  //
  //  val evaluater = new MulticlassClassificationEvaluator().setLabelCol("target").setPredictionCol("fake_predict").setMetricName("accuracy")
  //
  //  val accuracy = evaluater.evaluate(predictions)
  //
  //  println(accuracy)


  val dfs = session.createDataFrame(Seq(
    ("fight looms, Republicans flip",
      "On Christmas day, Donald Trump announced that he would  be back to work  the following day, but he is golfing for the fourth day in a row. The former reality show star blasted former President Barack Obama for playing golf and now Trump is on track to outpace the number of golf games his predecessor played.Updated my tracker of Trump s appearances at Trump properties.71 rounds of golf including today s. At this pace, he ll pass Obama s first-term total by July 24 next year. https://t.co/Fg7VacxRtJ pic.twitter.com/5gEMcjQTbH  Philip Bump (@pbump) December 29, 2017 That makes what a Washington Post reporter discovered on Trump s website really weird, but everything about this administration is bizarre AF. The coding contained a reference to Obama and golf:  Unlike Obama, we are working to fix the problem   and not on the golf course.  However, the coding wasn t done correctly.The website of Donald Trump, who has spent several days in a row at the golf course, is coded to serve up the following message in the event of an internal server error: https://t.co/zrWpyMXRcz pic.twitter.com/wiQSQNNzw0  Christopher Ingraham (@_cingraham) December 28, 2017That snippet of code appears to be on all https://t.co/dkhw0AlHB4 pages, which the footer says is paid for by the RNC? pic.twitter.com/oaZDT126B3  Christopher Ingraham (@_cingraham) December 28, 2017It s also all over https://t.co/ayBlGmk65Z. As others have noted in this thread, this is weird code and it s not clear it would ever actually display, but who knows.  Christopher Ingraham (@_cingraham) December 28, 2017After the coding was called out, the reference to Obama was deleted.UPDATE: The golf error message has been removed from the Trump and GOP websites. They also fixed the javascript  =  vs  ==  problem. Still not clear when these messages would actually display, since the actual 404 (and presumably 500) page displays a different message pic.twitter.com/Z7dmyQ5smy  Christopher Ingraham (@_cingraham) December 29, 2017That suggests someone at either RNC or the Trump admin is sensitive enough to Trump s golf problem to make this issue go away quickly once people noticed. You have no idea how much I d love to see the email exchange that led us here.  Christopher Ingraham (@_cingraham) December 29, 2017 The code was f-cked up.The best part about this is that they are using the  =  (assignment) operator which means that bit of code will never get run. If you look a few lines up  errorCode  will always be  404          (@tw1trsux) December 28, 2017trump s coders can t code. Nobody is surprised.  Tim Peterson (@timrpeterson) December 28, 2017Donald Trump is obsessed with Obama that his name was even in the coding of his website while he played golf again.Photo by Joe Raedle/Getty Images."
      ,"","user",null.asInstanceOf[Integer])
  )).toDF("title","text","subject","date","target")

  //println(dfs)

  //val Array(train_data, test_data) = create_dataframe().randomSplit(Array(0.8,0.2))

  val df = dfs.union(test)

  df.show()

  val text_preprocessed = preprocess_data(df,"text")
  val text_TFIDF = tf_idf(text_preprocessed,"text")
  val final_df = text_TFIDF.withColumnRenamed("text_tfidf","features")
  val (indexer,assembler) = pipeline_stages(2)
  val title_preprocessed = preprocess_data(text_TFIDF,"title")
  val title_TFIDF = tf_idf(title_preprocessed,"title")

  title_TFIDF.show()

  val nvmodel = PipelineModel.load("src/test/scala/resources/model/NaiveBayes")

  val model = PipelineModel.load("src/test/scala/resources/model/RandomForest")

  val nv_prediction = nvmodel.transform(title_TFIDF)

  val prediction = model.transform(title_TFIDF)


  nv_prediction.filter((col("date") === "user")).show(true)
  val nv_json = nv_prediction.filter((col("date") === "user")).select("prediction","probability").toJSON
  println("RF ******************")
  prediction.filter((col("date") === "user")).show(true)
  val rf_json = prediction.filter((col("date") === "user")).select("prediction","probability").toJSON

  nv_json.show(false)
  rf_json.show(false)

}