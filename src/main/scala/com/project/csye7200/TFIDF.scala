package com.project.csye7200
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
object TFIDF{

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
//  def naiveBayes(dataFrame: DataFrame,indexer: StringIndexer,assembler: VectorAssembler): Unit ={
//    val Array(train_data, test_data) = dataFrame.randomSplit(Array(0.7,0.3))
//    val nb = new NaiveBayes().setLabelCol("target").setFeaturesCol("features")
//    //val pipeline = new Pipeline().setStages(Array(indexer,assembler,nb))
//    val pipeline = new Pipeline().setStages(Array(assembler,nb))
//    val model = pipeline.fit(train_data)
//
//    val predictions = model.transform(test_data)
//    predictions.show()
//
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("target")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//
//    val accuracy = evaluator.evaluate(predictions)
//
//    println(s"Naive Bayes Test set accuracy = $accuracy")
//
//    model.write.overwrite().save("src/test/scala/resources/model/NaiveBayes")
//  }
//  def randomForestClassifier(dataFrame: DataFrame,indexer: StringIndexer,assembler: VectorAssembler): Unit ={
//
//    // Splitting Data Frame
//    val Array(train_data, test_data) = dataFrame.randomSplit(Array(0.7,0.3))
//
//    ///// Random Forest Classifier
//    val rf = new RandomForestClassifier()
//      .setLabelCol("target")
//      .setFeaturesCol("features")
//      .setNumTrees(20)
//      .setPredictionCol("prediction")
//      .setMaxDepth(7)
//
//    //val pipeline = new Pipeline().setStages(Array(indexer,assembler,rf))
//    val pipeline = new Pipeline().setStages(Array(assembler,rf))
//    val model = pipeline.fit(train_data)
//
//    val predictions = model.transform(test_data)
//    predictions.show()
//
//    // Select (prediction, true label) and compute test error.
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("target")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val accuracy = evaluator.evaluate(predictions)
//
//    println("Random Forest test set Accuracy = "+accuracy)
//
//    model.write.overwrite().save("src/test/scala/resources/model/RandomForest")
//
//  }

  def bulidMLModel(dataFrame: DataFrame, mlModel:String): Unit ={

    // Splitting Data Frame
    val Array(train_data, test_data) = dataFrame.randomSplit(Array(0.7,0.3))

    // Random Forest Classifier
    val rf = new RandomForestClassifier()
      .setLabelCol("target")
      .setFeaturesCol("features")
      .setNumTrees(20)
      .setPredictionCol("prediction")
      .setMaxDepth(7)

    // Naive Bayes Classifier
    val nb = new NaiveBayes()
      .setLabelCol("target")
      .setFeaturesCol("features")

    //val pipeline = new Pipeline().setStages(Array(indexer,assembler,rf))
    val pipeline = new Pipeline()

    mlModel match {
      case "rf_simple" => {
        val (_,assembler) = pipeline_stages(1)
        pipeline setStages (Array(assembler,rf))
      }
      case "nb_simple" => {
        val (_,assembler) = pipeline_stages(1)
        pipeline setStages (Array(assembler,nb))
      }
      case "rf_with_title" => {
        val (_,assembler) = pipeline_stages(2)
        pipeline setStages (Array(assembler, rf))
      }
      case "nb_with_title" => {
        val (_,assembler) = pipeline_stages(2)
        pipeline setStages (Array(assembler, nb))
      }
      case "rf_with_subject" => {
        val (indexer,assembler) = pipeline_stages(3)
        pipeline setStages (Array(indexer, assembler, rf))
      }
      case "nb_with_subject" => {
        val (indexer,assembler) = pipeline_stages(3)
        pipeline setStages (Array(indexer, assembler, nb))
      }
    }
    // Building the ML Model
    val model = pipeline.fit(train_data)

    val predictions = model.transform(test_data)
    predictions.show()

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("target")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    // Calculating accuracy as per test values
    val accuracy = evaluator.evaluate(predictions)

    // Print and save as per the model type specified
    mlModel match {
      case "rf_simple" => {
        println("Random Forest test set Accuracy = "+accuracy)
        model.write.overwrite().save("src/test/scala/resources/model/RandomForest")
      }
      case "nb_simple" => {
        println(s"Naive Bayes test set accuracy = $accuracy")
        model.write.overwrite().save("src/test/scala/resources/model/NaiveBayes")
      }
      case "rf_with_title" => {
        println(s"Random Forest test set with title  accuracy = $accuracy")
        model.write.overwrite().save("src/test/scala/resources/model/RandomForestTT")
      }
      case "nb_with_title" => {
        println(s"Naive Bayes test set with title accuracy = $accuracy")
        model.write.overwrite().save("src/test/scala/resources/model/NaiveBayesTT")
      }
      case "rf_with_subject" => {
        println(s"Random Forest test set with title and subject accuracy = $accuracy")
        model.write.overwrite().save("src/test/scala/resources/model/RandomForestSTT")
      }
      case "nb_with_subject" => {
        println(s"Naive Bayes test set with title and subject accuracy = $accuracy")
        model.write.overwrite().save("src/test/scala/resources/model/NaiveBayesSTT")
      }
    }
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