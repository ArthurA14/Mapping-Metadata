import org.apache.hadoop.fs.{FileSystem, Path}
import java.nio.file.FileSystems
import org.apache.hadoop.conf.Configuration
import java.io.{FileNotFoundException, IOException}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

/////////////////////////// GET THE MAPPING OF METADATA ////////////////////////////

def getSubDirectories(hdfsDirectory: String): List[String] = {
    val fs = FileSystem.get(new Configuration())
    val status = fs.listStatus(new Path(hdfsDirectory))
    status.filter(_.isDirectory).map(_.getPath.toString).toList
}


// def getColNames(subDirectory: String): List[(String, String, String)] = {
//   //val df = spark.read.parquet(subDirectory)
//   //val struct = df.dtypes
//   //val colNameList = struct.map(f => f._1.toString).toList
//   //struct.map(x => (subDirectory, x._1, x._2)).toList
  
//   //println("hello")
//   spark.read.parquet(subDirectory).dtypes.map(x => println(subDirectory, x._1, x._2))
//   //println("good bye")

// }
def getColNames(subDirectory: String): List[(String, String, String, Boolean)] = {
  val df = spark.read.parquet(subDirectory) 
  val struct = df.dtypes
  val colNameList = struct.map(_._1.toString).toList

  if (colNameList.contains("mobile_") || colNameList.contains("email") || colNameList.contains("nom"))
    struct.map(x => (subDirectory, x._1, x._2, true)).toList
  else if (!colNameList.contains("mobile_") && !colNameList.contains("email") && !colNameList.contains("nom"))
    struct.map(x => (subDirectory, x._1, x._2, false)).toList
  else
    List((null, null, null, false))

}


// def parquetMapping(hdfsDirectory: String): Unit = {
    
//     val fs = FileSystem.get(new Configuration())
//     val listDirectories = getSubDirectories(hdfsDirectory)
    
//     listDirectories.foreach {
//         s => 
//             try {
//                 getColNames(s)
//             } catch {
//                 case e1: Exception => parquetMapping(s)
//                 case e2: Error => parquetMapping(s)
//             }
//     }
// }
def parquetMapping(hdfsDirectory: String): String = {  
    val fs = FileSystem.get(new Configuration())
    //val hdfsDirectory2 : String = hdfsDirectory + "/data/clean"

    val listDirectories = getSubDirectories(hdfsDirectory)

    var newlist: List[(String, String, String, Boolean)] = List() // List((null, null, null, false))
    var newString: String = ""
    
    listDirectories.foreach { s => 
        //println("hello")
                try {
                    if (s != null) {   // listDirectories(k)
                        
                        for (j <- 0 to getColNames(s).length - 1)   // getColNames(listDirectories(k))
                            newlist = newlist :+ getColNames(s)(j)
                    } 
                    else {
                        List((null, null, null, false)) 
                    }
                } catch {
                    case e1: Exception => parquetMapping(s)
                    case e2: Error => parquetMapping(s)
                }

        //println(s)    
        //println("good bye")
    }
    newlist.foreach(el => newString = newString + el.toString + "\n")
    newString

}

parquetMapping("/user/allieart/testPath")

  > res26: String =
    "(hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,source_donnee_id,IntegerType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,individu_id,IntegerType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,adresse_postale_id,IntegerType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,npai_date,StringType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,ldm_filename,StringType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,ldm_process_date,DateType,false)
    (hdfs://endor.l******.local:8..."

/////////////////////////// WRITE THE SOLUTION INTO A FILE ////////////////////////////

object Hdfs extends App {
 
  def writeToFile(filePath: String) = {
    System.setProperty("HADOOP_USER_NAME", "AAA")
    val path = new Path(filePath + "/file.txt")
    val conf = new Configuration()
    //conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    val outputString = parquetMapping(filePath)
    val data = outputString.getBytes
    os.write(data)
    fs.close()
  }
}

Hdfs.writeToFile("/user/allieart/testPath/")

    > defined object Hdfs

    > (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,source_donnee_id,IntegerType,false)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,individu_id,IntegerType,false)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,adresse_postale_id,IntegerType,false)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,npai_date,StringType,false)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,ldm_filename,StringType,false)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,ldm_process_date,DateType,false)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,provider,StringType,true)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,channel,StringType,true)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,source_donnee_id,IntegerType,true)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,event,StringType,true)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,event_date,DateType,true)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,email,StringType,true)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,mobile_number,StringType,true)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,ldm_filename,StringType,true)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,ldm_anonymized,BooleanType,true)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,ldm_anonymized_at,TimestampType,true)
    > (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,ldm_process_date,DateType,true)