name := "SBT"

version := "0.1"

scalaVersion := "2.11.11"

lazy val spark = "2.2.0"
resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.apache.spark" %% "spark-mllib" % spark,
  "com.databricks" % "spark-csv_2.10" % "1.4.0",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1"% "test",
  "org.ansj" % "ansj_seg" % "5.1.5",
  "org.nlpcn" % "nlp-lang" % "1.7.7"
)
