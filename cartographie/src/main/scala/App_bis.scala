package com.l******

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SparkSession}

import java.net.URI



object App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  final val log: Logger = Logger.getLogger(this.getClass)


  def main(args : Array[String]) {
    // to use when compiling with IDE
     /*System.setProperty("hadoop.home.dir", "C:\\hadoop")*/

    val spark = SparkSession
      .builder()
      .appName("Export Logs")
      // .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val conf: Configuration = spark.sqlContext.sparkContext.hadoopConfiguration

    val hdfs_uri = args(2)
    //val hdfs_uri = "hdfs://endor.l*****.local:8020" 
    //db_logs = spark.read.option("header","true").option("inferSchema","true").option("recursiveFileLookup","true").csv(hdfs_uri+path+"/db_logs/")



    def getSubDirectories(hdfsDirectory: String): List[String] = {

      val fs = FileSystem.get(new URI(hdfs_uri), new Configuration())
      val status = fs.listStatus(new Path(hdfsDirectory))
      status.filter(_.isDirectory).map(x => x.getPath.toString).toList
    }


    def getColNames(subDirectory: String): List[(String, String, String)] = {
      val df = spark.read
        // .parquet(subDirectory)
        .option("basePath", subDirectory)
        .parquet(subDirectory + "/data/")
      val struct = df.dtypes
      val colNameList = struct.map(f => f._1.toString).toList

      struct.map(x => (subDirectory, x._1, x._2)).toList

    }


    def parquetMapping(hdfsDirectory: String): String = {
      val fs = FileSystem.get(new Configuration())

      val listDirectories = getSubDirectories(hdfsDirectory)

      var newList: List[(String, String, String)] = List()
      var newString: String = ""

      listDirectories.foreach { s =>
        try {
          if (s != null) {

            for (j <- 0 to getColNames(s).length - 1)
              newList = newList :+ getColNames(s)(j)
          }
        } catch {
          case e1: Exception => parquetMapping(s)
          case e2: Error => parquetMapping(s)
        }
      }
      newList.foreach(el => newString = newString + el.toString + "\n")
      newString

    }


    def writeToFile(confFilePath: String, pathToWrite: String) = {

      val df = spark.read
        .csv(confFilePath)

      val browseFile = df
        .as[String].collect()

      val listProjectPath = browseFile
        .map(projectNamePath => {

          var projectName = projectNamePath.split("/")(2)
          println("Traitement pour le projet : " + projectName + " en cours ...")

          val path = new Path(pathToWrite + "/" + projectName + ".txt")
          val conf = new Configuration()
          val fs = FileSystem.get(conf)
          val os = fs.create(path)

          val outputString = parquetMapping(projectNamePath).toString
          val data = outputString.getBytes
          os.write(data)
          fs.close()
          println("Traitement pour le projet " + projectName + " terminé")
        })
    }


    val conf_path = args(0) // "/projects/k*****/carte_cartographique/conf"
    val output_path = args(1) // "/projects/k*****/carte_cartographique/output"
    writeToFile(hdfs_uri + conf_path, hdfs_uri + output_path)
    //writeToFile(hdfs_uri + "/user/allieart/cartographie/conf", hdfs_uri + "/user/allieart/cartographie/output")

  }
}