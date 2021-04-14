import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
object Data extends App{
  def create_dataframe():DataFrame = {
    val session = SparkSession.builder()
      .master("local")
      .appName("Create DF using CSV")
      .getOrCreate()

    val real = session.read
      .options(Map("header" -> "true", "quote" -> "\"", "escape" -> "\""))
      .csv("src/test/scala/resources/datasets/true.csv")

    val fake = session.read
      .options(Map("header" -> "true", "quote" -> "\"", "escape" -> "\""))
      .csv("src/test/scala/resources/datasets/fake.csv")

    //real.withColumn("target", lit(0))
    //fake.withColumn("target", lit(1))

    val total_data = real.withColumn("target", lit(0))//.union(fake.withColumn("target", lit(1)))

    total_data.select("subject").distinct().show()
    println(total_data.count())

    total_data
  }
  create_dataframe().show(10)
}
