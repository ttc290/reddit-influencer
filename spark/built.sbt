name := "etl"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.4.2"
)