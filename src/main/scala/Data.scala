import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
object Data {
  def create_dataframe():DataFrame = {
    val session = SparkSession.builder()
      .master("local")
      .appName("Create DF using CSV")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    val real = session.read
      .options(Map("header" -> "true", "quote" -> "\"", "escape" -> "\""))
      .csv("src/test/scala/resources/datasets/true.csv")

    val fake = session.read
      .options(Map("header" -> "true", "quote" -> "\"", "escape" -> "\""))
      .csv("src/test/scala/resources/datasets/fake.csv")

    //real.withColumn("target", lit(0))
    //fake.withColumn("target", lit(1))

    val total_data = real.withColumn("target", lit(0)).union(fake.withColumn("target", lit(1)))

    total_data.select("subject").distinct().show()

    println(total_data.count())

    total_data
  }
  create_dataframe().show(10)

  /*
   * Cleaning the data - removing punctuations from the column position specified
   */
//  def clean_data(dataFrame: DataFrame, position: Int, newColumnName : String):DataFrame={
//    val test_with_no_punct: Seq[String] = dataFrame.collect().map(_.getString(position).replaceAll("https?://\\S+\\s?", "").
//      replaceAll("""[\p{Punct}]""", "")).toSeq
//
//    val rdd: RDD[String] = session.sparkContext.parallelize(test_with_no_punct)
//    val rdd_train: RDD[Row] = dataFrame.rdd.zip(rdd).map(r => Row.fromSeq(r._1.toSeq ++ Seq(r._2)))
//    val df = session.createDataFrame(rdd_train, dataFrame.schema.add(newColumnName, StringType))
//
//    df
//  }

  /*
   * Preprocessing of a specified column
   */
  def preprocess_data(dataFrame: DataFrame,columnName:String):DataFrame={

    val countTokens_train:UserDefinedFunction = udf { (words: Seq[String]) => words.length }

    // Extract tokens
    val tokenizer = new RegexTokenizer()
      .setInputCol(columnName)
      .setOutputCol(columnName+"_words")
      .setPattern("\\W")
    val token_df: DataFrame = tokenizer.transform(dataFrame)
    token_df.select(columnName,columnName+"_words").withColumn("tokens",countTokens_train(col(columnName+"_words"))).show()

    // Remove stop words

    val sw_remover = new StopWordsRemover()
      .setInputCol(columnName+"_words")
      .setOutputCol(columnName+"_sw_removed")
    sw_remover.transform(token_df)
    val sw_remover_df: DataFrame = sw_remover.transform(token_df).withColumn("tokens", countTokens_train(col(columnName+"_sw_removed")))

    sw_remover_df
  }
}


