package de.hpi.dataset_versioning.crawl.data.parser

import com.google.gson.{JsonArray, JsonElement, JsonNull, JsonObject, JsonPrimitive}

import scala.collection
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

//TODO: handle arrays and get attribute order!
class RelationalDataset(val rows:ArrayBuffer[Seq[JsonElement]]=ArrayBuffer()) {

  val nestedLevelSeparator = "_"

  var colNames = IndexedSeq[String]()
  private var colNameSet = Set[String]()
  private var containedNestedObjects = false
  var containsArrays = false
  private var erroneous = false


  def setSchema(firstObj:JsonObject) = {
    colNames = extractNestedKeyValuePairs(firstObj)
        .map(_._1)
    colNameSet = colNames.toSet
  }

  def setSchema(array:JsonArray) = {
    val it = array.iterator()
    val finalSchema = mutable.HashSet[String]()
    while (it.hasNext) {
      val curObj = it.next().getAsJsonObject
      val schema = extractNestedKeyValuePairs(curObj)
      finalSchema ++= schema.map(_._1)
    }
    colNames = finalSchema.toIndexedSeq.sorted
    colNameSet = finalSchema.toSet
  }

  private def extractNestedKeyValuePairs(firstObj: JsonObject): IndexedSeq[(String,JsonElement)] = extractNestedKeyValuePairs("",firstObj)

  private def extractNestedKeyValuePairs(prefix:String, firstObj: JsonObject): IndexedSeq[(String,JsonElement)] = {
    val keyPrefix = if(prefix.isEmpty) "" else prefix + nestedLevelSeparator
    firstObj.keySet().asScala.flatMap(k => {
      firstObj.get(k) match {
        case e:JsonArray => {
          containsArrays = true
          IndexedSeq((keyPrefix + k,e))
        }
        case e:JsonPrimitive => IndexedSeq((keyPrefix + k,e))
        case e:JsonObject => {
          containedNestedObjects = true
          extractNestedKeyValuePairs(keyPrefix + nestedLevelSeparator + k, firstObj.get(k).getAsJsonObject)
        }
        case _ => {
          erroneous = true
          throw new AssertionError("unmatched case")
        }
      }
    }).toIndexedSeq.sortBy(_._1)
  }

  def isEmpty = colNames.isEmpty || rows.isEmpty

  def appendRow(obj: JsonObject,strictSchemaCompliance:Boolean = false) = {
    val kvPairs = extractNestedKeyValuePairs(obj)
      .toMap
    if(!kvPairs.keySet.subsetOf(colNameSet) || (strictSchemaCompliance && kvPairs.keySet!=colNameSet)) {
      erroneous=true
      throw new SchemaMismatchException
    }
    val row = colNames.map(k => kvPairs.getOrElse(k,JsonNull.INSTANCE))
    rows += row
  }
}
