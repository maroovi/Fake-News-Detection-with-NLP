import sbt.Keys.scalaVersion

lazy val root = (project in file("."))
  .settings(
    name := "Fake-News-Detection-with-NLP",
    organization := "com.example",
    version := "0.1",
    scalaVersion := "2.12.12",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-mllib" % "2.4.0",
      "org.scalatest" %% "scalatest" % "3.2.2" % "test",
      "org.apache.spark" %% "spark-core" % "2.4.0",
      "org.apache.spark" %% "spark-sql" % "2.4.0",
      "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
      "org.twitter4j" % "twitter4j-core" % "4.0.4",
      "org.twitter4j" % "twitter4j-stream" % "4.0.4",
      //"org.vegas-viz" %% "vegas_2.11" % "0.3.11",
      //"org.vegas-viz" %% "vegas-spark_2.11" % "0.3.11",
      "org.plotly-scala" %% "plotly-core" % "0.8.0"

    ),
    dependencyOverrides += "com.google.guava" % "guava" % "15.0",
    scalacOptions ++= Seq(
      "-feature",
      "-deprecation",
      "-Xfatal-warnings"
    )
  )