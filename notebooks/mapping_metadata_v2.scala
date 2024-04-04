
/////////////////////////// SIMPLE SOLUTION ////////////////////////////
////////////////////////////////////////////////////////////////////////

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


// def getColNames(subDirectory: String): Unit = {
//   spark.read
//     // .parquet(subDirectory)
//     .option("basePath", subDirectory)
//     .parquet(subDirectory + "/data/")
//     .dtypes
//     .map(x => println(subDirectory, x._1, x._2))
// }
def getColNames(subDirectory: String): List[(String, String, String)] = {
    val df = spark.read
    // .parquet(subDirectory)
    .option("basePath", subDirectory)
    .parquet(subDirectory + "/data/")
    val struct = df.dtypes
    val colNameList = struct.map(f => f._1.toString).toList

    struct.map(x => (subDirectory, x._1, x._2)).toList

}


def parquetMapping(hdfsDirectory: String): Unit = {
    
    val fs = FileSystem.get(new Configuration())
    val listDirectories = getSubDirectories(hdfsDirectory)
    
    listDirectories.foreach {
        s => 
            try {
                getColNames(s)
            } catch {
                case e1: Exception => parquetMapping(s)
                case e2: Error => parquetMapping(s)
            }
    }
}

//parquetMapping("/projects/ch*****/data/clean")
parquetMapping("/user/allieart/testPath")

> (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,source_donnee_id,IntegerType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,individu_id,IntegerType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,adresse_postale_id,IntegerType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,npai_date,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,ldm_filename,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,ldm_process_date,DateType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,provider,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,channel,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,source_donnee_id,IntegerType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,event,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,event_date,DateType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,email,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,mobile_number,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,ldm_filename,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,ldm_anonymized,BooleanType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,ldm_anonymized_at,TimestampType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/ass***_one***,ldm_process_date,DateType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,numero_ticket,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,code_magasin,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,code_caisse,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,id_ticket,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,code_client,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,civilite,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,name,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,firstname,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,email,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,telephone,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,date_ticket,DateType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,montant_ticket,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,ldm_process_date,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,ldm_filename,StringType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,ldm_processed_at,TimestampType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,ldm_anonymized,BooleanType)
> (hdfs://endor.l******.local:8020/user/allieart/testPath/l****_gift_receipt/data,ldm_anonymized_at,TimestampType)


///////////////////////////  WRITE THE SOLUTION INTO A FILE ////////////////////////////

object Hdfs extends App {
 
  def writeToFile(confFilePath: String) = {
    System.setProperty("HADOOP_USER_NAME", "AAA")

    val df = spark.read
        .csv(confFilePath)

    val browseFile = df
        .as[String].collect()

    val listProjectPath = browseFile
        .map(projectNamePath => {
            // val projectName = projectNamePath.toString.substring(15)
            
            // var k = 0; var j = 0; var str = ""
            // var newstr = projectNamePath.substring(1)
            // newstr = newstr.substring(newstr.indexOf("/"), newstr.length)

            // for (k <- 0 to 1) {
                
            //     newstr = newstr.substring(0, newstr.length - str.length - 1)
            //     j = 0; str = "";
            //     while (! (newstr.reverse(j).toString == "/") ) {
            //         str = str + newstr.reverse(j).toString
            //         j = j + 1
            //     }
            // }

            // val projectName = newstr.substring(1, newstr.length - str.length - 1)

            val projectName = projectNamePath.split("/")(2)
            println("Traitement pour le projet : " + projectName + " en cours ...")
            
            val path = new Path("/user/allieart/mapFiles/" + projectName+".txt")
            val conf = new Configuration()
             //conf.set("fs.defaultFS", uri)
             val fs = FileSystem.get(conf)
             val os = fs.create(path)
             
            val outputString = parquetMapping(projectNamePath).toString
            val data = outputString.getBytes
            os.write(data)
            fs.close()
        })
  }
}

Hdfs.writeToFile("/user/allieart/cartographie/conf")

> defined object Hdfs

(null,null,null,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,source_donnee_id,IntegerType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,individu_id,IntegerType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,adresse_postale_id,IntegerType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,npai_date,StringType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,ldm_filename,StringType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,ldm_process_date,DateType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,provider,StringType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,channel,StringType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,source_donnee_id,IntegerType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,event,StringType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,event_date,DateType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,email,StringType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,mobile_number,StringType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,ldm_filename,StringType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,ldm_anonymized,BooleanType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,ldm_anonymized_at,TimestampType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,ldm_process_date,DateType,true)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,date,DateType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoCYP,DoubleType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoEEK,DoubleType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoLTL,DoubleType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoLVL,DoubleType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoMTL,DoubleType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoSIT,DoubleType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoSKK,DoubleType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,ldm_filename,StringType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,ldm_processed_at,TimestampType,false)
(hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,ldm_process_date,DateType,false)


/////////////////////////// COMPLEX SOLUTION ///////////////////////////
////////////////////////////////////////////////////////////////////////

import org.apache.hadoop.fs.{FileSystem, Path}
import java.nio.file.FileSystems
import org.apache.hadoop.conf.Configuration
import java.io.{FileNotFoundException, IOException}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

/////////////////////////// GET THE MAPPING OF METADATA ////////////////////////////

def getSubDirectories(hdfsDirectory: String): List[String] = {
    val fs = FileSystem.get(new Configuration())
    val status = fs.listStatus(new Path(hdfsDirectory))
    status.filter(_.isDirectory).map(x => x.getPath.toString).toList
}


def getColNames(subDirectory: String): List[(String, String, String, Boolean)] = {
  val df = spark.read
    // .parquet(subDirectory)
    .option("basePath", subDirectory)
    .parquet(subDirectory + "/data/")
  val struct = df.dtypes
  val colNameList = struct.map(_._1.toString).toList

  if (colNameList.contains("mobile_") || colNameList.contains("email") || colNameList.contains("nom"))
    struct.map(x => (subDirectory, x._1, x._2, true)).toList
  /*else if*/else (!colNameList.contains("mobile_") && !colNameList.contains("email") && !colNameList.contains("nom"))
    struct.map(x => (subDirectory, x._1, x._2, false)).toList
  /*else
    List((null, null, null, false))*/

}


def parquetMapping(hdfsDirectory: String): String = {  
    val fs = FileSystem.get(new Configuration())
    //val hdfsDirectory2 : String = hdfsDirectory + "/data/clean"

    val listDirectories = getSubDirectories(hdfsDirectory)

    var newList: List[(String, String, String, Boolean)] = List() // List((null, null, null, false))
    var newString: String = ""
    
    listDirectories.foreach { s => 
        //println("hello")
                try {
                    if (s != null) {   // listDirectories(k)
                        
                        for (j <- 0 to getColNames(s).length - 1)   // getColNames(listDirectories(k))
                            newList = newList :+ getColNames(s)(j)
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
    newList.foreach(el => newString = newString + el.toString + "\n")
    newString

}

parquetMapping("/user/allieart/testPath")

  > res5: String =
    //"(null,null,null,false)
    "(hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,source_donnee_id,IntegerType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,individu_id,IntegerType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath/all_*****,adresse_postale_id,IntegerType,fals..."


///////////////////////////  WRITE THE SOLUTION INTO A FILE ////////////////////////////

// object Hdfs extends App {
 
//   def writeToFile(confFilePath: String) = {
  def writeToFile(confFilePath: String, pathToWrite: String) = {
    // System.setProperty("HADOOP_USER_NAME", "AAA")

    val df = spark.read
        .csv(confFilePath)

    val browseFile = df
        .as[String].collect()

    val listProjectPath = browseFile
        .map(projectNamePath => {
            // val projectName = projectNamePath.toString.substring(15)  // /user/allieart/testPath -> testPath
            
            // var k = 0; var j = 0; var str = ""
            // var newstr = projectNamePath.substring(1)
            // newstr = newstr.substring(newstr.indexOf("/"), newstr.length)

            // for (k <- 0 to 1) {
                
            //     newstr = newstr.substring(0, newstr.length - str.length - 1)
            //     j = 0; str = "";
            //     while (! (newstr.reverse(j).toString == "/") ) {
            //         str = str + newstr.reverse(j).toString
            //         j = j + 1
            //     }
            // }

            // val projectName = newstr.substring(1, newstr.length - str.length - 1)  // /projects/ch*****/data/clean -> ch*****

            val projectName = projectNamePath.split("/")(2)
            println("Traitement pour le projet : " + projectName + " en cours ...")
            
            // val path = new Path("/user/allieart/mapFiles/" + projectName + ".txt")
            // val path = new Path("/projects/" + projectName + "/profiling/parquet_file_structure.csv")
            val path = new Path(pathToWrite + "/" + projectName + ".txt")
            val conf = new Configuration()
            //conf.set("fs.defaultFS", uri)
            val fs = FileSystem.get(conf)
            val os = fs.create(path)
             
            val outputString = parquetMapping(projectNamePath).toString
            val data = outputString.getBytes
            os.write(data)
            fs.close()
        })
  }
// }

// Hdfs.writeToFile("/user/allieart/cartographie/conf")
// writeToFile("/projects/k*****/carte_carthographique/conf", "/projects/k*****/carte_carthographique/output")
writeToFile("/user/allieart/cartographie/conf", "/user/allieart/cartographie/output")

    // > defined object Hdfs
    writeToFile: (confFilePath: String, pathToWrite: String)Unit

    // (null,null,null,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,source_donnee_id,IntegerType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,individu_id,IntegerType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,adresse_postale_id,IntegerType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,npai_date,StringType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,ldm_filename,StringType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/all_*****,ldm_process_date,DateType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,provider,StringType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,channel,StringType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,source_donnee_id,IntegerType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,event,StringType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,event_date,DateType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,email,StringType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,mobile_number,StringType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,ldm_filename,StringType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,ldm_anonymized,BooleanType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,ldm_anonymized_at,TimestampType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/ass***_one***,ldm_process_date,DateType,true)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,date,DateType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoCYP,DoubleType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoEEK,DoubleType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoLTL,DoubleType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoLVL,DoubleType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoMTL,DoubleType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoSIT,DoubleType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,EURtoSKK,DoubleType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,ldm_filename,StringType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,ldm_processed_at,TimestampType,false)
    (hdfs://endor.l******.local:8020/user/allieart/testPath_bis/b****,ldm_process_date,DateType,false)