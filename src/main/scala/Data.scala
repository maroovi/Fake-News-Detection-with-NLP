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

    real.withColumn("target", lit("real"))
    fake.withColumn("target", lit("fake"))

    val total_data = real//real.union(fake)

    total_data.select("subject").distinct().show()
    println(total_data.count())

    total_data
  }
}
