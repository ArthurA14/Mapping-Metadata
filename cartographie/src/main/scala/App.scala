package com.l*****

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}

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

    val hdfs_uri = args(1)
    //val hdfs_uri = "hdfs://endor.l*****.local:8020" 
    //db_logs = spark.read.option("header","true").option("inferSchema","true").option("recursiveFileLookup","true").csv(hdfs_uri+path+"/db_logs/")



    def getSubDirectories(hdfsDirectory: String): List[String] = {

      val fs = FileSystem.get(new URI(hdfs_uri), new Configuration())
      val status = fs.listStatus(new Path(hdfsDirectory))
      status.filter(_.isDirectory).map(x => x.getPath.toString).toList
    }


    def getColNames(subDirectory: String): List[(String, String, String, String)] = {
      val df = spark.read
        // .parquet(subDirectory)
        .option("basePath", subDirectory)
        .parquet(subDirectory + "/data/")
      val struct = df.dtypes
      // val colNameList = struct.map(f => f._1.toString).toList

      val lastUpdateDate = df.select($"ldm_process_date").orderBy($"ldm_process_date".desc).limit(1).as[String].collect().mkString

      struct.map(x => (subDirectory, x._1, x._2, lastUpdateDate)).toList

    }


    def parquetMapping(hdfsDirectory: String): DataFrame = {  
        val fs = FileSystem.get(new Configuration())

        val listDirectories = getSubDirectories(hdfsDirectory)

        var newlist: List[(String, String, String, String)] = List() // var newlist: List[(String, String, String)] = List() 
        var newString: String = ""

        listDirectories.foreach { s => 
                    try {
                        if (s != null) {   
                            
                            for (j <- 0 to getColNames(s).length - 1)   
                                newlist = newlist :+ getColNames(s)(j)
                        } 
                    } catch {
                        case e1: Exception => parquetMapping(s)
                        case e2: Error => parquetMapping(s)
                    }
        }
        import spark.implicits._
        newlist.toDF("files_path", "column_name", "data_type", "ldm_process_date")  // newlist.toDF("Data Flow", "Field", "Type")

    }


    def writeToFile(projectNamePath: String): Unit = {

        var projectName = projectNamePath.split("/")(4)
        println("Traitement pour le projet : " + projectName + " en cours ...")
        
        val outputString = parquetMapping(projectNamePath).coalesce(1).write
            .option("header",true)
            .option("delimiter",";")
            .mode("overwrite")
            .format("csv")
            .csv("/projects/" + projectName + "/profiling/parquet_file_structure.csv")
        println("Traitement pour le projet " + projectName + " terminé")
    }


    val project_path = args(0) // "/projects/d*****/data/clean"
    writeToFile(hdfs_uri + project_path)
    //writeToFile("hdfs://endor.l*****.local:8020" + "/projects/d*****/data/clean")

  }
}