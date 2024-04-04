
import java.text.DecimalFormat
import scala.util.Try

object UsualTransformations extends Serializable {

  def ratio(a: Double, b: Double) : Double = {
    val ratio = 100 * (a.toDouble / b.toDouble)
    castToDouble(ratio)
  }

  def castToDouble (x: Double) : Double = {
    val formatString = "#####.#"
    val formatter = new DecimalFormat(formatString)
    val result = formatter.format(x)
    result.toDouble
  }

  def isEmpty(x: String) : Boolean = {
    if (Option(x) == None) return false  
    x.trim == ""
  }

  def isNum(x: String) : Boolean = {
    val z: Option[Float] = Try(x.toFloat).toOption
    z != None
  }

  def fieldLen(x: Any): Int = {

    if (Option(x) == None) return 0  

    val stringVal: String = x match {
      case x: String => x
      case x: Integer => x.toString()
      case x: Long => x.toString()
      case _ => throw new Exception("We can't cast value in fieldMaxLen")
    }
    stringVal.length

  }

  val udfIsEmpty = udf[Boolean,String](isEmpty)
  val udfisNum = udf[Boolean, String](isNum)
  val udfFieldLen = udf[Int, String](fieldLen)

}

///////////////////////////////////////////////////////////////////////////////////////

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import UsualTransformations._

case class ColProfile (colName: String, nbOfLines: Long, singleVal: Long, emptyStringVal : Long, 
                        nullVal: Long, numVal: Long, 
                          fieldMaxLen: Int, fieldMinLen: Int
                      )
{

  lazy val percentageOfFilled: Double = ratioOfFilled(nullVal, emptyStringVal, nbOfLines)
  lazy val percentageOfNum: Double =  ratioOfNum(numVal, nbOfLines)

  def colOfData : List[String]= {
    List(colName, nbOfLines, singleVal, emptyStringVal, 
      nullVal, percentageOfFilled, percentageOfNum,
        fieldMaxLen, fieldMinLen
    ).map(x => x.toString)
  }

  def ratioOfFilled(nullVal: Long, emptyStringVal: Long, totalNbOfLines: Long): Double = {
    val filledRecords = totalNbOfLines - (nullVal + emptyStringVal)
    ratio(filledRecords, totalNbOfLines)
  }

  def ratioOfNum(numVal: Long, totalNbOfLines: Long): Double = {
    ratio(numVal, totalNbOfLines)
  }

  override def toString : String = {
    List(colName, nbOfLines, singleVal, emptyStringVal, 
          nullVal, percentageOfFilled, percentageOfNum, 
            fieldMaxLen.toString, fieldMinLen.toString
    ).mkString(",")
  }
}

object ColProfile{
  def ColProfileFunc(df: DataFrame, colName : String): ColProfile = {
    val dfColumn = df
      .select(colName)

    dfColumn.cache

    val nbOfLines = dfColumn
      .count()

    val singleVal = dfColumn
      .distinct()
      .count()

    val emptyCount = dfColumn
      .withColumn("isEmpty", udfIsEmpty(column(colName)))
      .filter(column("isEmpty") === true)
      .count()

    val nullCount = dfColumn
      .withColumn("isNull", column(colName).isNull)
      .filter(column("isNull"))
      .count()

    val numericCount = dfColumn
      .withColumn("isNum", udfisNum(column(colName)))
      .filter(column("isNum") === true)
      .count()

    val fieldMaxLen = dfColumn
      .withColumn("fieldLen", udfFieldLen(column(colName)))
      .agg(max(column("fieldLen")))
      .collect()(0)(0)
      .toString.toInt

    val fieldMinLen = dfColumn
      .withColumn("fieldLen", udfFieldLen(column(colName)))
      .agg(min(column("fieldLen")))
      .collect()(0)(0)
      .toString.toInt

    new ColProfile(colName, nbOfLines, singleVal, emptyCount, 
                    nullCount, numericCount, 
                    fieldMaxLen, fieldMinLen
    )
  }
}

///////////////////////////////////////////////////////////////////////////////////////

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class DFProfile(df: DataFrame) {

  df.cache

  lazy val spark = SparkSession
    .builder()
    .appName("Datalake Profiling")
    //.master("local[*]")
    .getOrCreate()

  val ColProfiles : List[ColProfile] =
    for (c <- df.columns.toList) yield ColProfile.ColProfileFunc(df, c)

  val header : List[String] = List("Column Name", "Number of Lines", "Single Values", 
                                    "Empty String Values", "Null Values", "Percentage of Filled", 
                                      "Percentage of Numeric", "Max Length", "Min Length"
  )

  def toDF : DataFrame = {
    def dfWithHeader(data: List[List[String]], header: String) : DataFrame = {
      val allRows = data.map(x => Row(x:_*))
      val rdd = spark.sparkContext.parallelize(allRows)
      val headerList = header.split(",")
      val schema = StructType(
        headerList.map(fieldName => StructField(fieldName, StringType, true))
      )
      // spark.sqlContext.createDataFrame(rdd, schema)
      val df = spark.createDataFrame(rdd, schema)
      df
    }
    val colData = ColProfiles.map(x => x.colOfData)
    dfWithHeader(colData, header.mkString(","))
  }

  override def toString : String = {
    val colProfileToString : List[String] = ColProfiles.map(x => x.toString)
    val prettyColProfile = (header.mkString(",") :: ColProfiles).mkString("\n")
    prettyColProfile
  }
}

///////////////////////////////////////////////////////////////////////////////////////

import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame

object DFUtils {
  implicit class DFProfiling(df: DataFrame) {
    def dfProfile : DataFrame = {
      DFProfile(df).toDF
    }
  }
}


///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////
/////////////////////// KEYWORD SEARCHING AND PROFILING FUNCTION //////////////////////

import DFUtils._
// import org.apache.spark.sql.functions.col
//Replace part of a string using regexp_replace()
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.typedLit

def searchingAndProfiling(fileAbsoluthPath: String): Unit = {
    
    // val newfileAbsoluthPath = fileAbsoluthPath.substring(20)
    
    // ////// 1 //////
    // var str = ""
    // var j = 0
    // while (! (fileAbsoluthPath.reverse(j).toString == "/") ) {
    //     str = str + fileAbsoluthPath.reverse(j).toString
    //     j = j + 1
    // }
    // var substrPath = fileAbsoluthPath.slice(str.indexOf("/"), fileAbsoluthPath.length - str.length)

    // ////// 2 //////
    // var newstr = substrPath.substring(1) 
    // str = ""
    // j = 0
    // while (! (newstr(j).toString == "/") ) {
    //     str = str + newstr(j).toString
    //     j = j + 1
    // }
    // substrPath = newstr.slice(newstr.indexOf("/"), newstr.length - str.length + 8)

    // ////// 3 //////
    // newstr = substrPath.substring(1) 
    // str = ""
    // j = 0
    // while (! (newstr(j).toString == "/") ) {
    //     str = str + newstr(j).toString
    //     j = j + 1
    // }
    // substrPath = newstr.slice(newstr.indexOf("/"), newstr.length - str.length + 9)

    // ////// 4 //////
    // newstr = substrPath.substring(1)
    // str = ""
    // j = 0
    // while (! (newstr(j).toString == "/") ) {
    //     str = str + newstr(j).toString
    //     j = j + 1
    // }
    // substrPath = newstr.slice(newstr.indexOf("/"), newstr.length - str.length + 4)

    // ////// 5 //////
    // newstr = substrPath.substring(1, substrPath.length - 1) 
    // str = ""
    // j = 0
    // while (! (newstr.reverse(j).toString == "/") ) {
    //     str = str + newstr.reverse(j).toString
    //     j = j + 1
    // }
    // substrPath = newstr.slice(0, newstr.length - str.length - 1)

    // ////// 6 //////
    // newstr = substrPath  
    // var str2 = ""
    // j = 0
    // while (! (newstr.reverse(j).toString == "/") ) {
    //     str2 = str2 + newstr.reverse(j).toString
    //     j = j + 1
    // }
    // substrPath = newstr.slice(0, newstr.length - str2.length - 1)

    val substrPath = fileAbsoluthPath.split("/")(4) + "/" + fileAbsoluthPath.split("/")(5)

    val parqDF = spark.read.parquet(fileAbsoluthPath)
    val allcolNames = parqDF.columns

    var g = 0

    for (k <- 0 to allcolNames.length - 1) {
        
        var col = allcolNames(k) // colonne k
        // println("col = " + col + ": " + col.length)
                
        // var g = 0
        
        val myWordList = List("mobile_", "email", "nom")
        
        for (j <- 0 to myWordList.length - 1) {

            var i = 0

            while ( i < col.length - 1 && !( col.contains(myWordList(j)) ) ) {
                myWordList(j).concat(col(i).toString)
                i = i + 1
            }
            while ( col.contains(myWordList(j)) && g < 1 ) {
                // parqDF.dfProfile.withColumn("substrPath", regexp_replace($"Empty Strings", "0", substrPath)).show
                // parqDF.dfProfile.withColumn("substrPath", lit(substrPath)).show
                // parqDF.dfProfile.withColumn("substrPath", lit(fileAbsoluthPath)).show
                parqDF.dfProfile
                  .withColumn("substrPath", lit(substrPath))
                  .show
                g = g + 1
            }
            
        }
        
    }

}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////
////////////////////// GET LIST OF FILES HDFS Root directory FUNCTION /////////////////

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.nio.file.FileSystems
import org.apache.hadoop.conf.Configuration

import java.io.{FileNotFoundException, IOException}

def listFileNamesRoot(hdfsDirectory: String): List[String] = {
    val fs = FileSystem.get(new Configuration())
    //println(fs + "\n")
    
    try {
    
        fs.listStatus(new Path(hdfsDirectory))
          .flatMap { s =>
            // If it's a file :
            if (s.isFile && (s.getPath.getName.endsWith(".parquet")) )
                List(hdfsDirectory + "/" + s.getPath.getName)
            else if (s.isFile && !(s.getPath.getName.endsWith(".parquet")) )
                List[String]()
            //else if (!s.isFile && (s.getPath.getName.endsWith(".db")) )
            //    throw new FileNotFoundException()
            // If it's a dir and we're in a recursive option :
            else
                listFileNamesRoot(hdfsDirectory + "/" + s.getPath.getName)
          }
          .toList
          .sorted
    
    } catch {
        case _: FileNotFoundException =>
            null
        //case e: IOException => 
        //    throw new FileNotFoundException()
    }

}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////
///////////////////////// FINAL PARQUET PROFILING HDFS FUNCTION ///////////////////////

//%spark2

def parquetProfiling(directory: String): Unit = {
    val listFiles = listFileNamesRoot(directory)
    listFiles.foreach{ x =>
        // println(x) 
        val result = searchingAndProfiling(x)
        println(result)
    }
}

///////////////////////////////////////////////////////////////////////////////////////

val directory = "/projects/c******/data/clean/assainissement_oneshot/"
//val directory = "/user/allieart/"

parquetProfiling(directory)

+-----------------+---------------+-------------+-------------------+-----------+--------------------+---------------------+----------+----------+--------------------+
|      Column Name|Number of Lines|Single Values|Empty String Values|Null Values|Percentage of Filled|Percentage of Numeric|Max Length|Min Length|          substrPath|
+-----------------+---------------+-------------+-------------------+-----------+--------------------+---------------------+----------+----------+--------------------+
|         provider|          53443|            1|                  0|          0|               100.0|                  0.0|         7|         7|clean/assainissem...|
|          channel|          53443|            1|                  0|          0|               100.0|                  0.0|         5|         5|clean/assainissem...|
| source_donnee_id|          53443|            1|                  0|          0|               100.0|                100.0|         2|         2|clean/assainissem...|
|            event|          53443|            2|                  0|          0|               100.0|                  0.0|        11|        10|clean/assainissem...|
|       event_date|          53443|         1499|                  0|          1|               100.0|                  0.0|        10|         0|clean/assainissem...|
|            email|          53443|        51443|                  0|          0|               100.0|                  0.0|        64|        64|clean/assainissem...|
|    mobile_number|          53443|            1|                  0|      53443|                 0.0|                  0.0|         0|         0|clean/assainissem...|
|     ldm_filename|          53443|            2|                  0|          0|               100.0|                  0.0|        14|        10|clean/assainissem...|
|   ldm_anonymized|          53443|            1|                  0|          0|               100.0|                  0.0|         4|         4|clean/assainissem...|
|ldm_anonymized_at|          53443|            1|                  0|          0|               100.0|                  0.0|        23|        23|clean/assainissem...|
+-----------------+---------------+-------------+-------------------+-----------+--------------------+---------------------+----------+----------+--------------------+