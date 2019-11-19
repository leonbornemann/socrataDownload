package de.hpi.dataset_versioning.crawl.data.parser

import java.io.{File, FileReader, PrintWriter, StringReader}

import com.google.gson.{JsonParser, JsonPrimitive}
import com.google.gson.stream.JsonReader
import de.hpi.dataset_versioning.crawl.data.parser.exceptions.FirstElementNotObjectException

import scala.collection.JavaConverters._
import scala.collection.mutable

class JsonDataParser {

  def parseAllJson(dir:File) = {
    val files = dir.listFiles()
      .filter(_.getName.endsWith(".json?"))
    var firstElementNotObjectExceptionCount = 0
    var schemaMismatchExceptionCount = 0
    var memberIsArrayException = 0
    var numSuccessful = 0
    var otherException = 0
    val schemata = mutable.HashSet[Set[String]]()
    files.foreach(f => {
      try{
        val ds = parseJsonFile(f)
        schemata += ds.colNames.toSet
        numSuccessful+=1
      } catch{
        case e:FirstElementNotObjectException => firstElementNotObjectExceptionCount+=1
        case e:SchemaMismatchException => schemaMismatchExceptionCount+=1
        case e:MemberIsArrayException => memberIsArrayException+=1
        case _ => otherException+=1
      }
    })
    println(s"numSuccessful:$numSuccessful")
    println(s"firstElementNotObjectExceptionCount: $firstElementNotObjectExceptionCount")
    println(s"schemaMismatchExceptionCount: $schemaMismatchExceptionCount")
    println(s"memberIsArrayException: $memberIsArrayException")
    println(s"otherException: $otherException")
    println(s"distinct Schemata:  ${schemata.size}")
    val pr = new PrintWriter("/home/leon/data/dataset_versioning/socrata/schemaSizes.csv")
    schemata.foreach(s => pr.println(s.size))
    pr.close()
  }

  //TODO: include known metadata!
  def parseJsonFile(file:File,strictSchema:Boolean=false):RelationalDataset = {
    val reader = new JsonReader(new FileReader(file))
    val parser = new JsonParser();
    var array = parser.parse(reader).getAsJsonArray
    val it = array.iterator()
    if(!it.hasNext){
      reader.close()
      new RelationalDataset() //return empty dataset
    } else {
      if(strictSchema) {
        //everything must conform to the schema of the first row
        val first = it.next()
        if (!first.isJsonObject) throw new FirstElementNotObjectException()
        val obj = first.getAsJsonObject
        val ds = new RelationalDataset()
        ds.setSchema(obj)
        ds.appendRow(obj)
        while (it.hasNext) {
          val curObj = it.next().getAsJsonObject
          ds.appendRow(curObj)
        }
        reader.close()
        ds
      } else{
        //every row that does not have a field that other rows gets a missing value inserted
        val ds = new RelationalDataset()
        ds.setSchema(array)
        while (it.hasNext) {
          val curObj = it.next().getAsJsonObject
          ds.appendRow(curObj)
        }
        reader.close()
        ds
      }
    }
  }

}
