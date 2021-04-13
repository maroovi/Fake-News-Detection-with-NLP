import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF, RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import Data._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.spark_project.dmg.pmml.True
object TFIDF extends App {

  val sparkSession = SparkSession.builder // To create a spark session
    .master("local")
    .appName("TFIDF")
    .getOrCreate()

  val dfs = create_dataframe()

  val test_with_no_punct: Seq[String] = dfs.collect().map(_.getString(3).replaceAll("https?://\\S+\\s?", "").
    replaceAll("""[\p{Punct}]""", "")).toSeq

  ///////

  val rdd: RDD[String] = sparkSession.sparkContext.parallelize(test_with_no_punct)
  val rdd_train: RDD[Row] = dfs.rdd.zip(rdd).map(r => Row.fromSeq(r._1.toSeq ++ Seq(r._2)))
  val df = sparkSession.createDataFrame(rdd_train, dfs.schema.add("new_text", StringType))

  SQLConf.get.setConfString("LEGACY_ALLOW_UNTYPED_SCALA_UDF","true")



  //val title_tokenizer_word = new Tokenizer().setInputCol("title").setOutputCol("title_words")

  //val title_tokenized = title_tokenizer_word.transform(df)

  //title_tokenized.select("title","title_words").withColumn("tokens",countTokens_train(col("title_words")) )

  val countTokens_train:UserDefinedFunction = udf { (words: Seq[String]) => words.length }

  val title_tokenizer = new RegexTokenizer() // Extract tokens from title
    .setInputCol("new_text")
    .setOutputCol("title_words")
    .setPattern("\\W")

  val title_token_df: DataFrame = title_tokenizer.transform(df)

  title_token_df.select("new_text","title_words").withColumn("tokens",countTokens_train(col("title_words"))).show(false)

  ///////////////////////

  val title_sw_remover = new StopWordsRemover() // Remove stop words from title
    .setInputCol("title_words")
    .setOutputCol("title_sw_removed")

  title_sw_remover.transform(title_token_df)

  val title_sw_remover_df: DataFrame = title_sw_remover.transform(title_token_df).withColumn("tokens", countTokens_train(col("title_sw_removed")))

  ///////////////


  val title_count_vectorizer: CountVectorizerModel = new CountVectorizer() // Compute Term frequency from title
    .setInputCol("title_tokenizer")
    .setOutputCol("tf_title")
    .setVocabSize(3)
    .setMinDF(2)
    .fit(title_sw_remover_df)

  val title_count_vectorizer_df: DataFrame= title_count_vectorizer.transform(title_sw_remover_df)

  val idf = new IDF().setInputCol("tf_title").setOutputCol("title_tfidf")

  val idfModel = idf.fit(title_count_vectorizer_df)

  val rescaledData = idfModel.transform(title_count_vectorizer_df)

  rescaledData.show(10)
}
