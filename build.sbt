name := "hss-spark"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= {
  val sparkV = "1.5.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-streaming" % sparkV,
    "org.apache.spark" %% "spark-streaming-kafka" % sparkV,
    "org.elasticsearch" % "elasticsearch" % "2.3.1",
    "org.elasticsearch" %% "elasticsearch-spark" % "2.3.1",
    "com.twitter" %% "chill-bijection" % "0.8.0"
  )
}
