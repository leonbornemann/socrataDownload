package de.hpi.dataset_versioning.data

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.google.gson._
import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.experiment.example_query_imputation.join.JoinConstructionVariant.JoinConstructionVariant
import de.hpi.dataset_versioning.data.metadata.custom.joinability.`export`.Column
import de.hpi.dataset_versioning.data.diff.TupleMatcher
import de.hpi.dataset_versioning.data.metadata.custom.{ColumnCustomMetadata, CustomMetadata}
import de.hpi.dataset_versioning.data.parser.exceptions.SchemaMismatchException
import de.hpi.dataset_versioning.experiment.example_query_imputation.join.JoinConstructionVariant
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.util.TableFormatter

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class LoadedRelationalDataset(val id:String, val version:LocalDate, val rows:ArrayBuffer[IndexedSeq[JsonElement]]=ArrayBuffer()) extends StrictLogging{

  def getRowMultiSet = rows.groupBy(identity)
    .mapValues(_.size)

  def isEqualTo(other: LoadedRelationalDataset, mapping: mutable.HashMap[String, String]): Boolean = {
    //we need to compare all tuples in the order specified by the mapping
    if(other.ncols != ncols || mapping.size!=ncols)
      false
    else {
      val tuplesToMatch = mutable.HashMap() ++ other.getRowMultiSet
      var allTuplesFound = true
      val it = rows.iterator
      val otherColNameToIndex = other.colNames.zipWithIndex.toMap
      while (it.hasNext && allTuplesFound) {
        val curRow = it.next()
        val curRowInOtherOrder = mutable.IndexedSeq.fill[JsonElement](curRow.size)(JsonNull.INSTANCE)
        (0 until curRow.size).foreach(myColIndex => {
          val myColName = colNames(myColIndex)
          val otherColIndex = otherColNameToIndex(mapping(myColName))
          curRowInOtherOrder(otherColIndex) = curRow(myColIndex)
        })
        val countInOtherDS = tuplesToMatch.getOrElse(curRowInOtherOrder, 0)
        if (countInOtherDS == 0)
          allTuplesFound = false
        else if(countInOtherDS > 1)
          tuplesToMatch(curRowInOtherOrder) = countInOtherDS - 1
        else
          tuplesToMatch.remove(curRowInOtherOrder) //we have matched all tuple duplicates
      }
      allTuplesFound && tuplesToMatch.isEmpty
    }
  }


  def getSchemaSpecificHashValue: Int = {
    columnMetadata.toIndexedSeq.sortBy(_._1).hashCode()
  }

  def toMultiset[A](list: Seq[A]) = list.groupBy(identity).mapValues(_.size)

  def getTupleSpecificHash: Int = {
    val tupleMultiset = toMultiset(rows
      .map(tuple => toMultiset(tuple.map(getCellValueAsString(_)))))
      /*.sorted(new Ordering[Seq[String]] {
      override def compare(x: Seq[String], y: Seq[String]): Int = {
        val it = x.zip(y).iterator
        var res = 0
        while(it.hasNext && res==0){
          val (s1,s2) = it.next()
          res = s1.compareTo(s2)
        }
        res
      }
    })*/
    tupleMultiset.hashCode()
  }

  def extractCustomMetadata(intID:Int) = {
    calculateColumnMetadata()
    CustomMetadata(id,intID,version,rows.size,getSchemaSpecificHashValue,getTupleSpecificHash,columnMetadata.toMap)
  }

  def join(other: LoadedRelationalDataset, myJoinColIndex: Short, otherJoinColIndex: Short,variant:JoinConstructionVariant):LoadedRelationalDataset = {
    val myJoinCol = colNames(myJoinColIndex)
    val otherJoinCol = other.colNames(otherJoinColIndex)
    val joinDataset = new LoadedRelationalDataset(id + s"_joinedOn($myJoinCol,$otherJoinCol)_with_" +other.id ,version)
    if(variant == JoinConstructionVariant.KeepBoth) {
      joinDataset.colNames = colNames ++ other.colNames
    } else if (variant == JoinConstructionVariant.KeepLeft){
      joinDataset.colNames = colNames ++ other.colNames.filter(_ != otherJoinCol)
    } else{
      joinDataset.colNames = colNames.filter(_ != myJoinCol) ++ other.colNames.filter(_ != otherJoinCol)
    }
    joinDataset.colNameSet = joinDataset.colNames.toSet
    joinDataset.erroneous = erroneous || other.erroneous
    joinDataset.containsArrays = containsArrays || other.containsArrays
    joinDataset.containedNestedObjects = containedNestedObjects | other.containedNestedObjects
    val byKey = other.rows.groupBy(c => getCellValueAsString(c(otherJoinColIndex)))
    rows.foreach(r => {
      val valueInJoinColumn = getCellValueAsString(r(myJoinColIndex))
      byKey.getOrElse(valueInJoinColumn,Seq())
        .foreach(matchingRow => {
          if(variant == JoinConstructionVariant.KeepBoth) {
            joinDataset.rows += r ++ matchingRow
          } else if (variant == JoinConstructionVariant.KeepLeft){
            joinDataset.rows += r ++ matchingRow.slice(0,otherJoinColIndex) ++ matchingRow.slice(otherJoinColIndex+1,matchingRow.size)
          } else{
            joinDataset.rows += r.slice(0,myJoinColIndex) ++ r.slice(myJoinColIndex,r.size) ++ matchingRow
          }
        })
    })
    joinDataset
  }


  def isSorted(colNames: IndexedSeq[String]): Boolean = {
    var sorted = true
    var it = colNames.iterator
    var prev = it.next()
    while(it.hasNext && sorted){
      val cur = it.next()
      if(cur < prev)
        sorted = false
      prev = cur
    }
    sorted
  }

  def calculateColumnMetadata() = {
    assert(isSorted(colNames))
    columnMetadata.clear()
    columnMetadata ++= (0 until ncols).map(i => {
      val curColObject = getColumnObject(i)
      (colNames(i),ColumnCustomMetadata(colNames(i),i.toShort,curColObject.valueMultiSet.hashCode(),curColObject.uniqueness(),curColObject.dataType()))
    })
  }

  /***
   * Checks tuple set equality (assuming same order of columns in both datasets)
   *
   * @param other
   * @return
   */
  def tupleSetEqual(other: LoadedRelationalDataset): Boolean = {
    rows.toSet == other.rows.toSet
  }


  def getProjection(newID:String, columnRenames: IndexedSeq[(String,String)]) = {
    val (myColNames,projectedColnames) = columnRenames.unzip
    assert(myColNames.toSet.subsetOf(colNameSet))
    val projected = new LoadedRelationalDataset(newID,version)
    projected.colNames = projectedColnames
    projected.colNameSet = projectedColnames.toSet
    projected.containedNestedObjects = containedNestedObjects
    projected.containsArrays = containsArrays
    projected.erroneous = erroneous
    val columnIndexMap = colNames.zipWithIndex.toMap
    for(i <- 0 until rows.size) {
      val newRow = ArrayBuffer[JsonElement]()
      for (myName <- myColNames) {
        val j = columnIndexMap(myName)
        newRow += rows(i)(j)
      }
      projected.rows +=newRow
    }
    projected
  }


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
  var columnMetadata = mutable.HashMap[String,ColumnCustomMetadata]()


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

  def ncols = colNames.size

}
object LoadedRelationalDataset {
  val NULL_VALUE = ""
}
