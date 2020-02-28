package de.hpi.dataset_versioning.data.parser

import java.io.{File, FileReader, PrintWriter, StringReader}

import com.google.gson.{JsonParser, JsonPrimitive}
import com.google.gson.stream.JsonReader
import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.LoadedRelationalDataset
import de.hpi.dataset_versioning.data.parser.exceptions.{ContainsArrayException, FirstElementNotObjectException, SchemaMismatchException}

import scala.collection.JavaConverters._
import scala.collection.mutable

class JsonDataParser extends StrictLogging{

  def parseAllJson(dir:File) = {
    val files = dir.listFiles()
      .filter(_.getName.endsWith(".json?"))
    var firstElementNotObjectExceptionCount = 0
    var schemaMismatchExceptionCount = 0
    var containsArrayException = 0
    var numSuccessful = 0
    var otherException = 0
    val schemata = mutable.HashSet[Set[String]]()
    var numParsed = 0
    files.foreach(f => {
      if(numParsed % 1000==0) {
        logger.debug(s"Parsed $numParsed files")
        printParseSummary(firstElementNotObjectExceptionCount, schemaMismatchExceptionCount, containsArrayException, numSuccessful, otherException, schemata)
      }
      try{
        val ds = parseJsonFile(f)
        schemata += ds.colNames.toSet
        numSuccessful+=1
      } catch{
        case e:FirstElementNotObjectException => firstElementNotObjectExceptionCount+=1
        case e:SchemaMismatchException => schemaMismatchExceptionCount+=1
        case e:ContainsArrayException => containsArrayException+=1
        case _:Throwable => otherException+=1
      }
      numParsed +=1
    })
    printParseSummary(firstElementNotObjectExceptionCount, schemaMismatchExceptionCount, containsArrayException, numSuccessful, otherException, schemata)
    val pr = new PrintWriter("/home/leon/data/dataset_versioning/socrata/schemaSizes.csv")
    schemata.foreach(s => pr.println(s.size))
    pr.close()
  }

  private def printParseSummary(firstElementNotObjectExceptionCount: Int, schemaMismatchExceptionCount: Int, memberIsArrayException: Int, numSuccessful: Int, otherException: Int, schemata: mutable.HashSet[Set[String]]) = {
    println(s"numSuccessful:$numSuccessful")
    println(s"firstElementNotObjectExceptionCount: $firstElementNotObjectExceptionCount")
    println(s"schemaMismatchExceptionCount: $schemaMismatchExceptionCount")
    println(s"memberIsArrayException: $memberIsArrayException")
    println(s"otherException: $otherException")
    println(s"distinct Schemata:  ${schemata.size}")
  }

  //TODO: include known metadata!
  def parseJsonFile(file:File,strictSchema:Boolean=false,allowArrays:Boolean=true):LoadedRelationalDataset = {
    val reader = new JsonReader(new FileReader(file))
    val parser = new JsonParser();
    val array = parser.parse(reader).getAsJsonArray
    val it = array.iterator()
    if(!it.hasNext){
      reader.close()
      new LoadedRelationalDataset() //return empty dataset
    } else {
      var dataset:LoadedRelationalDataset = null
      if(strictSchema) {
        //everything must conform to the schema of the first row
        val first = it.next()
        if (!first.isJsonObject)
          throw new FirstElementNotObjectException()
        val obj = first.getAsJsonObject
        val ds = new LoadedRelationalDataset()
        ds.setSchema(obj)
        ds.appendRow(obj)
        while (it.hasNext) {
          val curObj = it.next().getAsJsonObject
          ds.appendRow(curObj)
        }
        reader.close()
        dataset = ds
      } else{
        //every row that does not have a field that other rows gets a missing value inserted
        val ds = new LoadedRelationalDataset()
        ds.setSchema(array)
        while (it.hasNext) {
          val curObj = it.next().getAsJsonObject
          ds.appendRow(curObj)
        }
        reader.close()
        dataset = ds
      }
      if(dataset.containsArrays && !allowArrays)
        throw new ContainsArrayException
      dataset
    }
  }

}
