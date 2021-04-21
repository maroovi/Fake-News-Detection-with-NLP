package com.project.csye7200
import Data._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import plotly._
import plotly.element._

import scala.collection.mutable

object Vizu extends App{

  val ipdata = create_dataframe()
  val real_data: Dataset[Row] = ipdata.filter("target == 0")
  val fake_data: Dataset[Row] = ipdata.filter("target == 1")

  val real_count = real_data.groupBy("subject").count()

  real_count.show(10)


  //val real_sub = real_count.select("subject").collect().map(row=>row.toSeq).toSeq

  val real_sub = real_count.select("subject").map(_(0))

  real_sub.foreach((element) => println(element+" "))


  val fake_count = real_data.groupBy("subject").count()

  //fake_count.show(10)

 val data = Seq(
   Bar(
     Seq(real_count.select("subject")) ,
     Seq(real_count.select("count"))
   )
 )

}
