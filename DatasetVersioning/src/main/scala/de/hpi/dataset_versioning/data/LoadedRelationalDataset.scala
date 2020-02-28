package de.hpi.dataset_versioning.data

import com.google.gson._
import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.parser.exceptions.SchemaMismatchException
import de.hpi.dataset_versioning.util.TableFormatter

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//TODO: get attribute order!
class LoadedRelationalDataset(val rows:ArrayBuffer[Seq[JsonElement]]=ArrayBuffer()) extends StrictLogging{

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
