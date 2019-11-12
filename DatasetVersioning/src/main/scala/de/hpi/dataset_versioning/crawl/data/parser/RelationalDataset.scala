package de.hpi.dataset_versioning.crawl.data.parser

import com.google.gson.{JsonArray, JsonObject, JsonPrimitive}

import scala.collection
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class RelationalDataset(firstObj:JsonObject, val rows:ArrayBuffer[Seq[JsonPrimitive]]=ArrayBuffer()) {

  var colNames = IndexedSeq[String]()

  def extractNestedSchema(firstObj: JsonObject): IndexedSeq[String] = extractNestedSchema("",firstObj)

  def extractNestedSchema(prefix:String,firstObj: JsonObject): IndexedSeq[String] = {
    val keyPrefix = if(prefix.isEmpty) "" else prefix + "_"
    firstObj.keySet().asScala.flatMap(k => {
      firstObj.get(k) match { //TODO: fix switch case
        case e:JsonArray => throw new MemberIsArrayException
        case e:JsonPrimitive => IndexedSeq(keyPrefix + k)
        case e:JsonObject => extractNestedSchema(keyPrefix + "_" + k, firstObj.get(k).getAsJsonObject)
        case _ => throw new AssertionError("unmatched case")
      }
    }).toIndexedSeq.sorted
  }

  if(firstObj!=null)
    colNames = extractNestedSchema(firstObj)
  private val colNameSet = colNames.toSet
  private var containedNestedObjects = false
  private var erroneous = false

  def isEmpty = colNames.isEmpty || rows.isEmpty

  def appendRow(obj: JsonObject) = {
    if(extractNestedSchema(obj).toSet!=colNameSet) {
      erroneous=true
      println()
      println(extractNestedSchema(obj).sorted)
      println(colNameSet.toSeq.sorted)
      println()
      throw new SchemaMismatchException() //TODO: introduce new values and set all of the other ones to null
    }
    val row = extractScalarAttributes(obj)
    rows +=row
  }

  private def extractScalarAttributes(obj: JsonObject):IndexedSeq[JsonPrimitive] = {
    val row = obj.keySet().asScala.map(k => {
      val i = colNames.indexOf(k)
      (i, k)
    }).toIndexedSeq.sortBy(_._1)
      .flatMap { case (_, k) => {
        val curValue = obj.get(k)
        if (!curValue.isJsonPrimitive) {
          //we can try to resolve objects recursively if they do not contain any arrays
          containedNestedObjects=true
          if (curValue.isJsonArray) {
            erroneous=true
            throw new MemberIsArrayException()
          }
          assert(curValue.isJsonObject)
          extractScalarAttributes(curValue.getAsJsonObject)
        } else {
          IndexedSeq(curValue.getAsJsonPrimitive)
        }
      }}
    row
  }
}
