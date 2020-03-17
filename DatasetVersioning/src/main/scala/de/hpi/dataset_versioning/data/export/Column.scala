package de.hpi.dataset_versioning.data.`export`

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{JsonWritable, LoadedRelationalDataset}

import scala.collection.mutable.ArrayBuffer

case class Column(id: String, version: String, attrName: String, values:Seq[String]) extends JsonWritable[Column]{

  def uniqueness() = {
    values.toSet.size / values.size.toDouble
  }

  val numericThreshold = 0.9

  def isNumeric = {
    val it = values.iterator
    var certainlyNonNumeric = false
    var nonNumericCount = 0
    while (it.hasNext && !certainlyNonNumeric){
      val curValue = it.next()
      if(!isIntOrDoubleOrNull(curValue))
        nonNumericCount+=1
      certainlyNonNumeric = nonNumericCount / values.size.toDouble > (1.0-numericThreshold) //this way we can do an early abort
    }
    !certainlyNonNumeric
    //values.filter( s => isDouble(s)).size / values.size.toDouble >= numericThreshold
  }

  def toLSHEnsembleDomain = {
    LSHEnsembleDomain(id,version,attrName,values.toSet)
  }

  def isIntOrDoubleOrNull(string:String) = string == LoadedRelationalDataset.NULL_VALUE || string.matches("-?[0-9]+|-?[0-9]+[\\.,][0-9]+")

}
