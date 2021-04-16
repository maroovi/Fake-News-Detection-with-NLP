import Data.create_dataframe
import TFIDF.{nv_prediction, rescaledtextData, text_sw_remover_df}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, RegexTokenizer, StopWordsRemover, StringIndexer, VectorAssembler}
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}

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

  val test_with_no_punct: Seq[String] = df.collect().map(_.getString(3).replaceAll("https?://\\S+\\s?", "").
    replaceAll("""[\p{Punct}]""", "")).toSeq

  val countTokens_train:UserDefinedFunction = udf { (words: Seq[String]) => words.length }

  val title_tokenizer = new RegexTokenizer() // Extract tokens from title
    .setInputCol("title")
    .setOutputCol("title_words")
    .setPattern("\\W")

  val title_token_df: DataFrame = title_tokenizer.transform(df)

  title_token_df.select("title","title_words").withColumn("tokens",countTokens_train(col("title_words"))).show(false)

  val title_sw_remover = new StopWordsRemover() // Remove stop words from title
    .setInputCol("title_words")
    .setOutputCol("title_sw_removed")

  title_sw_remover.transform(title_token_df)

  val title_sw_remover_df: DataFrame = title_sw_remover.transform(title_token_df).withColumn("tokens", countTokens_train(col("title_sw_removed")))

  val title_count_vectorizer: CountVectorizerModel = new CountVectorizer() // Compute Term frequency from title
    .setInputCol("title_sw_removed")
    .setOutputCol("tf_title")
    .setVocabSize(3)
    .setMinDF(2)
    .fit(title_sw_remover_df)

  val title_count_vectorizer_df: DataFrame= title_count_vectorizer.transform(title_sw_remover_df)

  val idf = new IDF().setInputCol("tf_title").setOutputCol("title_tfidf")

  val idfModel = idf.fit(title_count_vectorizer_df)

  val rescaledData = idfModel.transform(title_count_vectorizer_df)

  rescaledData.show(10)

  /// text


  val text_tokenizer = new RegexTokenizer() // Extract tokens from text
    .setInputCol("text")
    .setOutputCol("text_words")
    .setPattern("\\W")

  val text_token_df: DataFrame = text_tokenizer.transform(rescaledData)

  text_token_df.select("text","text_words").withColumn("tokens",countTokens_train(col("text_words"))).show(false)

  val text_sw_remover = new StopWordsRemover() // Remove stop words from title
    .setInputCol("text_words")
    .setOutputCol("text_sw_removed")

  text_sw_remover.transform(text_token_df)

  val text_sw_remover_df: DataFrame = text_sw_remover.transform(text_token_df).withColumn("tokens", countTokens_train(col("text_sw_removed")))

  val text_count_vectorizer: CountVectorizerModel = new CountVectorizer() // Compute Term frequency from title
    .setInputCol("text_sw_removed")
    .setOutputCol("tf_text")
    .setVocabSize(3)
    .setMinDF(2)
    .fit(text_sw_remover_df)

  val text_count_vectorizer_df: DataFrame= text_count_vectorizer.transform(text_sw_remover_df)

  val idf_text = new IDF().setInputCol("tf_text").setOutputCol("text_tfidf")

  val idftextModel = idf_text.fit(text_count_vectorizer_df)

  val rescaledtextData = idftextModel.transform(text_count_vectorizer_df)


  ////

  val indexer = new StringIndexer().setInputCol("subject").setOutputCol("subject_idx")

  val assembler = new VectorAssembler().setInputCols(Array("title_tfidf","text_tfidf","subject_idx")).setOutputCol("features")


  //// Splitting Data Frame

  val nvmodel = PipelineModel.load("src/test/scala/resources/model/myRandomForestClassificationModels")

  val nv_prediction = nvmodel.transform(rescaledtextData)


  //val nv_evaluater = new MulticlassClassificationEvaluator().setLabelCol("target").setPredictionCol("prediction").setMetricName("accuracy")


  //val nv_accuracy = nv_evaluater.evaluate(nv_prediction)

  nv_prediction.filter((col("date") === "user")).show(true)

  //println(nv_accuracy)
}
