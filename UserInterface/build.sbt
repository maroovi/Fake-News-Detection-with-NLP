import sbt.Keys.scalaVersion

lazy val root = (project in file("."))
  .enablePlugins(PlayScala)
  .settings(
    name := "Fake-News-Detection-with-NLP",
    organization := "com.example",
    version := "0.1",
    scalaVersion := "2.12.12",
    libraryDependencies ++= Seq(
      guice,
      "org.scalatestplus.play" %% "scalatestplus-play" % "5.0.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.3" % "test",
    ),
    scalacOptions ++= Seq(
      "-feature",
      "-deprecation",
      "-Xfatal-warnings"
    )
  )