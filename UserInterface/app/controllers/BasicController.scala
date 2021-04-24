package controllers

import models.{InputTypesForm, ModelPrediction}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.data.Form
import play.api.data.Forms.{mapping, text}
import play.api.mvc._

import javax.inject.{Inject, Singleton}

@Singleton
class BasicController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with play.api.i18n.I18nSupport {

  def index() = Action { implicit request =>
    Ok(views.html.inputsTypesForm(InputTypesForm.form))
  }

  def inputTypesForm() = Action { implicit request =>
    Ok(views.html.inputsTypesForm(InputTypesForm.form))
  }

  def inputTypesFormPost() = Action { implicit request =>
    InputTypesForm.form.bindFromRequest.fold(
      formWithErrors => {
        // binding failure, you retrieve the form containing errors:
        BadRequest(views.html.inputsTypesForm(formWithErrors))
      },
      formData => {
        Ok("Hello World")
      }
    )
  }

  val modelForm:Form[ModelPrediction] = Form(
    mapping (
      "news" -> text,
      "subject" -> text,
      "model"-> text
    ) (ModelPrediction.apply)(ModelPrediction.unapply)
  )

  def analysisPost() = Action { implicit request =>
    val data = modelForm.bindFromRequest.get
    //val jsonData = model_prediction(data.news,data.model,data.subject)
    Ok(data.news+" "+data.subject+" "+data.model)
  }


  def model_prediction(text: String,model_type: String,subject:String):String={

    val session = SparkSession.builder()
      .master("local")
      .appName("Load Model")
      .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")

    val test = session.read
      .options(Map("header" -> "true", "quote" -> "\"", "escape" -> "\""))
      .csv("MLModel/test.csv")

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
        case "rf" => subject match{
          case "None" =>"MLModel/RandomForestSTT"
          case _ =>   "MLModel/RandomForest"
        }
        case "nb" => subject match {
          case "None" => "MLModel/NaiveBayes"
          case _ =>      "MLModel/NaiveBayesSTT"
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
