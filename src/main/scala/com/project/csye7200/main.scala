package com.project.csye7200
import Data._
import com.project.csye7200.TFIDF._
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

  bulidMLModel(dataFrame,"rf_simple")
  bulidMLModel(dataFrame,"nb_simple")
  bulidMLModel(dataFrame,"rf_with_title")
  bulidMLModel(dataFrame,"nb_with_title")
  bulidMLModel(dataFrame,"rf_with_subject")
  bulidMLModel(dataFrame,"nb_with_subject")

//  randomForestClassifier(dataFrame,indexer,assembler)
//  naiveBayes(dataFrame,indexer,assembler)


}