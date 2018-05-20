name := "project"

version := "0.1"

scalaVersion := "2.11.12"

val scalaVersion2 = "2.11.12"
val sparkVersion = "2.3.0"


libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
//  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",


//  "org.scala-lang" % "scala-reflect" % scalaVersion2 % "provided",

  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test"
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

mainClass in (Compile, run) := Some("net.am.kaggle.titanic.Explore")
