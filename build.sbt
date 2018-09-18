name := "HDFS"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVer = "2.3.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "compile" withSources(),
    "org.apache.spark" %% "spark-sql" % sparkVer % "compile" withSources()
  )
}
