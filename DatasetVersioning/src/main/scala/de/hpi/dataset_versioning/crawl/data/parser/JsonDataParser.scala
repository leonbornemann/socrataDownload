package de.hpi.dataset_versioning.crawl.data.parser

import java.io.{File, FileReader, StringReader}

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
    files.foreach(f => {
      try{
        val ds = parseJsonFile(f)
        numSuccessful+=1
      } catch{
        case e:FirstElementNotObjectException => firstElementNotObjectExceptionCount+=1
        case e:SchemaMismatchException => schemaMismatchExceptionCount+=1
        case e:MemberIsArrayException => memberIsArrayException+=1
      }
    })
    println(s"numSuccessful:$numSuccessful")
    println(s"firstElementNotObjectExceptionCount: $firstElementNotObjectExceptionCount")
    println(s"schemaMismatchExceptionCount: $schemaMismatchExceptionCount")
    println(s"memberIsArrayException: $memberIsArrayException")
  }

  //TODO: include known metadata!
  def parseJsonFile(file:File):RelationalDataset = {
    val reader = new JsonReader(new FileReader(file))
    val parser = new JsonParser();
    var array = parser.parse(reader).getAsJsonArray
    val dataset = new RelationalDataset(array)
    val it = array.iterator()
    if(!it.hasNext){
      reader.close()
      new RelationalDataset(null) //return empty dataset
    } else {
      val first = it.next()
      if(!first.isJsonObject) throw new FirstElementNotObjectException()
      val obj = first.getAsJsonObject
      val ds = new RelationalDataset(obj)
      ds.appendRow(obj)
      while (it.hasNext){
        val curObj = it.next().getAsJsonObject
        ds.appendRow(curObj)
      }
      reader.close()
      ds
    }
  }

}
