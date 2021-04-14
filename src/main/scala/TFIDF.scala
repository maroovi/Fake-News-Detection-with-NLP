import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF, RegexTokenizer, StopWordsRemover, StringIndexer, Tokenizer, VectorAssembler}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import Data._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.spark_project.dmg.pmml.True
object TFIDF extends App {

  val sparkSession = SparkSession.builder // To create a spark session
    .master("local")
    .appName("TFIDF")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  val df = create_dataframe()

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

  println("Text")
  rescaledtextData.show(10)


  ////

  val indexer = new StringIndexer().setInputCol("subject").setOutputCol("subject_idx")

  //val subject_str_indexed = indexer.fit(rescaledtextData).transform(rescaledtextData)  #########

  val assembler = new VectorAssembler().setInputCols(Array("title_tfidf","text_tfidf","subject_idx")).setOutputCol("features")

  //val vec_assembler = assembler.transform(subject_str_indexed)  #######

  //vec_assembler.show(10)


  //// Splitting Data Frame

  val Array(train_data, test_data) = rescaledtextData.randomSplit(Array(0.7,0.3))


  ///// Random Forest Classifier

  val rf = new RandomForestClassifier().setLabelCol("target").setFeaturesCol("features").setNumTrees(20).setPredictionCol("fake_predict").setMaxDepth(7)

  val pipeline = new Pipeline().setStages(Array(indexer,assembler,rf))

  val model = pipeline.fit(train_data)

  val predictions = model.transform(test_data)

  predictions.show(10)
}
