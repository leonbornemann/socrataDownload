package de.hpi.dataset_versioning.data

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.google.gson._
import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.`export`.Column
import de.hpi.dataset_versioning.data.diff.TupleMatcher
import de.hpi.dataset_versioning.data.parser.exceptions.SchemaMismatchException
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.util.TableFormatter

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class LoadedRelationalDataset(id:String, version:LocalDate, val rows:ArrayBuffer[Seq[JsonElement]]=ArrayBuffer()) extends StrictLogging{

  def getColumnObject(j: Int) = {
    val values:ArrayBuffer[String] = getColContentAsString(j)
    Column(id,version.format(IOService.dateTimeFormatter),colNames(j),values)
  }

  def exportToCSV(file:File) = {
    val pr = new PrintWriter(file)
    pr.println(toCSVLineString(colNames))
    rows.foreach( r => {
      pr.println(toCSVLineString(r.map(getCellValueAsString(_))))
    })
    pr.close()
  }

  private def toCSVLineString(line:Seq[String]) = {
    line.map("\"" + _ + "\"").mkString(",")
  }

  def getCellValueAsString(jsonValue:JsonElement):String = {
    jsonValue match {
      case primitive: JsonPrimitive => primitive.getAsString
      case array: JsonArray =>array.toString
      case _: JsonNull => LoadedRelationalDataset.NULL_VALUE
      case _ => throw new AssertionError("Switch case finds unhalndeld option")
    }
  }

  def getColContentAsString(j: Int) = {
    val values = scala.collection.mutable.ArrayBuffer[String]()
    for (i <- 0 until rows.size) {
      val jsonValue = rows(i)(j)
      values+= getCellValueAsString(jsonValue)
    }
    values
  }

  def exportColumns(writer:PrintWriter, skipNumeric:Boolean = true) = {
    if(!isEmpty && !erroneous) {
      for (i <- 0 until rows(0).size) {
        val col = getColumnObject(i)
        if(!skipNumeric || !col.isNumeric) {
          val json = col.toLSHEnsembleDomain.toJson()
          writer.println(json)
        }
      }
    }
  }

  def calculateDataDiff(other: LoadedRelationalDataset) = {
    val myTuples = getTupleMultiSet
    val otherTuples = other.getTupleMultiSet
    val tupleMatcher = new TupleMatcher()
    val diff = tupleMatcher.matchTuples(myTuples,otherTuples)
    //schema differences:
    if(other.colNameSet!=colNameSet){
      if((!other.colNameSet.diff(colNameSet).isEmpty))
        diff.schemaChange.projection = Some((colNames,other.colNames))
      val inserts = colNameSet.diff(other.colNameSet).toSeq.sorted
      if(!inserts.isEmpty)
        diff.schemaChange.columnInsert = Some(inserts)
    }
    diff
  }

  def getAsTableString(rows: Set[Set[(String, JsonElement)]]) = {
    val schema = rows.head.map(_._1).toIndexedSeq.sorted
    val content = rows.map(_.toIndexedSeq.sortBy(_._1).map(_._2)).toIndexedSeq
    TableFormatter.format(Seq(schema) ++ content)
  }

  def print() = {
    logger.debug("\n" + TableFormatter.format(Seq(colNames) ++ rows))
  }

  private def getTupleMultiSet = {
    rows.map(r => {
      assert(r.size == colNames.size)
      colNames.zip(r).toSet
    }).groupBy(identity)
  }

  val nestedLevelSeparator = "_"

  var colNames = IndexedSeq[String]()
  private var colNameSet = Set[String]()
  private var containedNestedObjects = false
  var containsArrays = false
  var erroneous = false


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
object LoadedRelationalDataset {
  val NULL_VALUE = ""
}
