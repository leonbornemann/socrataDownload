package de.hpi.dataset_versioning.data.diff

import com.google.gson.JsonElement
import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.util.TableFormatter

import scala.collection.mutable

class RelationalDatasetDiff(val inserts: mutable.HashSet[Set[(String, JsonElement)]],
                            val deletes: mutable.HashSet[Set[(String, JsonElement)]],
                            val updates:mutable.HashMap[Set[(String, JsonElement)],Set[(String, JsonElement)]],
                            var schemaChange: SchemaChange = new SchemaChange,
                            var incomplete:Boolean=false) extends StrictLogging{

  def getAsTableString(rows: scala.collection.Set[Set[(String, JsonElement)]]) = {
    if(rows.isEmpty)
      ""
    else {
      val schema = rows.head.map(_._1).toIndexedSeq.sorted
      val content = rows.map(_.toIndexedSeq.sortBy(_._1).map(_._2)).toIndexedSeq
      TableFormatter.format(Seq(schema) ++ content)
    }
  }

  def getUpdatesAsTableString() = {
    if(updates.isEmpty)
      ""
    else{
      val schemaLeft = updates.keySet.head.map(_._1).toIndexedSeq.sorted
      val schemaRight = updates.values.head.map(_._1).toIndexedSeq.sorted
      val schema = schemaLeft ++ Seq("-->") ++schemaRight
      val content = updates.map{case (l,r) => {
        val left = l.toIndexedSeq.sortBy(_._1).map(_._2)
        val right = r.toIndexedSeq.sortBy(_._1).map(_._2)
        left ++ Seq("-->") ++ right
      }}.toIndexedSeq
      TableFormatter.format(Seq(schema) ++ content)
    }
  }

  def print() = {
    logger.debug( //TODO: print updates as well
      s"""
         |-----------Schema Changes:--------------
         |${schemaChange.getAsPrintString}
         |----------------------------------------
         |-----------Data Changes-----------------
         |Inserts:
         |${getAsTableString(inserts)}
         |Deletes:
         |${getAsTableString(deletes)}
         |Updates:
         |${getUpdatesAsTableString()}
         |""".stripMargin
    )
  }
}
