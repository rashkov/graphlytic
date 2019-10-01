name := "graphlytic"

version := "1.0"

scalaVersion := "2.11.8"
scalaVersion in ThisBuild := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.0" % "provided",
  "net.debasishg" %% "redisclient" % "3.10",
  "com.databricks" %% "spark-xml" % "0.6.0"
)

