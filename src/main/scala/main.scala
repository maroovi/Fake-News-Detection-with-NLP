import Data._
import TFIDF._
import org.apache.spark.sql.DataFrame


object main extends App{

  def loadData():DataFrame={
    val df = create_dataframe()
    val text_preprocessed = preprocess_data(df,"text")
    val text_TFIDF = tf_idf(text_preprocessed,"text")
    val final_df = text_TFIDF.withColumnRenamed("text_tfidf","features")
    val title_preprocessed = preprocess_data(text_TFIDF,"title")
    val title_TFIDF = tf_idf(title_preprocessed,"title")

    title_TFIDF
  }

  val dataFrame = loadData()
  val (indexer,assembler) = pipeline_stages(2)

//  bulidMLModel(dataFrame,indexer,assembler,"")

  randomForestClassifier(dataFrame,indexer,assembler)
  naiveBayes(dataFrame,indexer,assembler)


}