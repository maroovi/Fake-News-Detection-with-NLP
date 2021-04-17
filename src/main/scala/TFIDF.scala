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
  def naiveBayes(dataFrame: DataFrame,indexer: StringIndexer,assembler: VectorAssembler): Unit ={
    val Array(train_data, test_data) = dataFrame.randomSplit(Array(0.7,0.3))
    val nb = new NaiveBayes()
      .setLabelCol("target")
    val pipeline = new Pipeline().setStages(Array(indexer,assembler,nb))
    val model = pipeline.fit(train_data)

    //model.write.overwrite().save("src/test/scala/resources/model/NaiveBayes")
    val predictions = model.transform(test_data)
    predictions.show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("target")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)

    println(s"Naive Bayes Test set accuracy = $accuracy")
  }
  def randomForestClassifier(dataFrame: DataFrame,indexer: StringIndexer,assembler: VectorAssembler): Unit ={

    // Splitting Data Frame
    val Array(train_data, test_data) = dataFrame.randomSplit(Array(0.7,0.3))

    ///// Random Forest Classifier
    val rf = new RandomForestClassifier()
      .setLabelCol("target")
      .setFeaturesCol("features")
      .setNumTrees(20)
      .setPredictionCol("fake_predict")
      .setMaxDepth(7)

    val pipeline = new Pipeline().setStages(Array(indexer,assembler,rf))
    val model = pipeline.fit(train_data)
    model.write.overwrite().save("src/test/scala/resources/model/RandomForestClassifier")
    val predictions = model.transform(test_data)
    predictions.show()

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("target")
      .setPredictionCol("fake_predict")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)

    println("Random Forest test set Accuracy = "+accuracy)

  }
  def pipeline_stages():(StringIndexer,VectorAssembler)={
    // Converting labels to label indices
    val indexer = new StringIndexer().setInputCol("subject").setOutputCol("subject_idx")

    // Joining all the transformed columns
    val assembler = new VectorAssembler().setInputCols(Array("title_tfidf","text_tfidf","subject_idx")).setOutputCol("features")
    (indexer,assembler)
  }
}
