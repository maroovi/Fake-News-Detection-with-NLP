import Data._
import TFIDF._


object main extends App{
  val df = create_dataframe()
  val text_preprocessed = preprocess_data(df,"text")
  val text_TFIDF = tf_idf(text_preprocessed,"text")
  val final_df = text_TFIDF.withColumnRenamed("text_tfidf","features")
  //val (indexer,assembler) = pipeline_stages()
  val (indexer,assembler) = pipeline_stages()
  val title_preprocessed = preprocess_data(text_TFIDF,"title")
  val title_TFIDF = tf_idf(title_preprocessed,"title")
  naiveBayes(title_TFIDF,indexer,assembler)
  randomForestClassifier(title_TFIDF,indexer,assembler)

}