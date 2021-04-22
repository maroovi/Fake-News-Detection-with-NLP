package sevice
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

import javax.inject.Singleton

trait IModelPredictionService {
  def model_prediction(text: String,model: String):String
}

@Singleton
class ModelPredictionService extends IModelPredictionService {
  def model_prediction(text: String,model_type: String):String={

    val session = SparkSession.builder()
      .master("local")
      .appName("Load Model")
      .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")

    val test = session.read
      .options(Map("header" -> "true", "quote" -> "\"", "escape" -> "\""))
      .csv("src/test/scala/resources/datasets/test.csv")

    val dfs = session.createDataFrame(Seq(
      (text,text,"","user",null.asInstanceOf[Integer])
    )).toDF("title","text","subject","date","target")

    val df = dfs.union(test)

    val text_preprocessed = preprocess_data(df,"text")
    val text_TFIDF = tf_idf(text_preprocessed,"text")
    val title_preprocessed = preprocess_data(text_TFIDF,"title")
    val title_TFIDF = tf_idf(title_preprocessed,"title")
    val model:PipelineModel = PipelineModel.load(
    model_type match {
      case "rf_simple" => {
        "src/test/scala/resources/model/RandomForest"
      }
      case "nb_simple" => {
       "src/test/scala/resources/model/NaiveBayes"
      }
      case "rf_with_title" => {
       "src/test/scala/resources/model/RandomForestTT"
      }
      case "nb_with_title" => {
        "src/test/scala/resources/model/NaiveBayesTT"
      }
      case "rf_with_subject" => {
        "src/test/scala/resources/model/RandomForestSTT"
      }
      case "nb_with_subject" => {
        "src/test/scala/resources/model/NaiveBayesSTT"
      }
    })

    val prediction = model.transform(title_TFIDF)

    val output_json = prediction.filter((col("date") === "user")).select("prediction","probability").toJSON.collect().mkString("")

    output_json
  }
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
  def tf_idf(dataFrame: DataFrame,columnName: String):DataFrame={
    // Compute Term frequency vectors
    val count_vectorizer: CountVectorizerModel = new CountVectorizer()
      .setInputCol(columnName+"_sw_removed")
      .setOutputCol(columnName+"_ct_vectorized")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(dataFrame)
    val count_vectorizer_df: DataFrame= count_vectorizer.transform(dataFrame)

    // Performing IDF for extracting features
    val idf = new IDF().setInputCol(columnName+"_ct_vectorized").setOutputCol(columnName+"_tfidf")
    val idfModel = idf.fit(count_vectorizer_df)
    val rescaledData = idfModel.transform(count_vectorizer_df)

    rescaledData
  }
  def pipeline_stages(mode:Int):(StringIndexer,VectorAssembler)={
    val indexer = new StringIndexer()        // Converting labels to label indices
    val assembler = new VectorAssembler()    // Joining all the transformed columns

    mode match {
      case 1 =>assembler setInputCols(Array("text_tfidf")) setOutputCol("features")
      case 2 =>  assembler setInputCols(Array("title_tfidf","text_tfidf")) setOutputCol("features")
      case 3 => {
        indexer setInputCol("subject") setOutputCol("subject_idx")
        assembler setInputCols(Array("title_tfidf","text_tfidf","subject_idx")) setOutputCol("features")
      }
    }
    (indexer,assembler)
  }
}
