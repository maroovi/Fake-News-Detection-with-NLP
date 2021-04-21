import Data.{create_dataframe, preprocess_data}
import TFIDF.{pipeline_stages, tf_idf}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ModelLoad extends App {

  val session = SparkSession.builder()
    .master("local")
    .appName("Load Model")
    .getOrCreate()

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
    ("Drunk Bragging Trump Staffer Started Russian Collusion Investigation",
      "House Intelligence Committee Chairman Devin Nunes is going to have a bad day. He s been under the assumption, like many of us, that the Christopher Steele-dossier was what prompted the Russia investigation so he s been lashing out at the Department of Justice and the FBI in order to protect Trump. As it happens, the dossier is not what started the investigation, according to documents obtained by the New York Times.Former Trump campaign adviser George Papadopoulos was drunk in a wine bar when he revealed knowledge of Russian opposition research on Hillary Clinton.On top of that, Papadopoulos wasn t just a covfefe boy for Trump, as his administration has alleged. He had a much larger role, but none so damning as being a drunken fool in a wine bar. Coffee boys  don t help to arrange a New York meeting between Trump and President Abdel Fattah el-Sisi of Egypt two months before the election. It was known before that the former aide set up meetings with world leaders for Trump, but team Trump ran with him being merely a coffee boy.In May 2016, Papadopoulos revealed to Australian diplomat Alexander Downer that Russian officials were shopping around possible dirt on then-Democratic presidential nominee Hillary Clinton. Exactly how much Mr. Papadopoulos said that night at the Kensington Wine Rooms with the Australian"
      ,"politicsNews","user",0)
  )).toDF("title","text","subject","date","target")

  println(dfs)

  val Array(train_data, test_data) = create_dataframe().randomSplit(Array(0.8,0.2))

  val df = dfs.union(test_data)

  val text_preprocessed = preprocess_data(df,"text")
  val text_TFIDF = tf_idf(text_preprocessed,"text")
  val final_df = text_TFIDF.withColumnRenamed("text_tfidf","features")
  val (indexer,assembler) = pipeline_stages()
  val title_preprocessed = preprocess_data(text_TFIDF,"title")
  val title_TFIDF = tf_idf(title_preprocessed,"title")

  val nvmodel = PipelineModel.load("src/test/scala/resources/model/myRandomForestClassificationModels")

  val nv_prediction = nvmodel.transform(title_TFIDF)



  nv_prediction.filter((col("date") === "user")).show(true)

}
