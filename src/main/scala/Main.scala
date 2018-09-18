import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
//import  org.apache.hadoop.fs.{FileSystem,Path}

object Main extends App {
  // turn off logger messages in INFO level
  // and only show messages in WARN level
  //Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  // Define Spark Session and Spark Context
  val Session = SparkSession
    .builder
    .appName("HDFS_List")
    .master("spark://192.168.88.52:7077")
    .config("spark.executor.memory", "4gb")
    .getOrCreate()

  // import spark implicits to import $ operations and ...
  import Session.implicits._

  val sc = Session.sparkContext

//  FileSystem.get(sc.hadoopConfiguration ).listStatus( new Path("hdfs:///usr/local"))
//    .foreach( x => println(x.getPath))
//val fs = FileSystem.get(new java.net.URI("hdfs://192.168.88.52:9000"), sc.hadoopConfiguration)
//  val status = fs.listStatus(new Path("hdfs://192.168.88.52:9000/user/root/data/ling-spam/"))
//  status.foreach(x=> println(x.getPath))


  val myFile = sc.textFile("hdfs://192.168.88.52:9000/user/root/data/ling-spam/ham/3-384msg1.txt", minPartitions = 10)
  myFile.collect().foreach(println)

}