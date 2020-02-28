package de.hpi.dataset_versioning.matching

import java.io.File

import scala.collection.mutable

class GroundTruthMatching extends MatchingAlgorithm {

  def isDatasetFile(str: String): Boolean = str.endsWith(".json")

  override def matchDatasets(previous: Set[DatasetInstance], current: Set[DatasetInstance]) = {
    val currentIds = current.map(d => (d.id,d))
        .toMap
    previous.foreach( d => {
      if(currentIds.contains(d.id)){
        matching.matchings.put(d,currentIds(d.id))
      } else{
        matching.deletes.add(d)
      }
    })
    currentIds.keySet.diff(previous.map(_.id)).foreach(d => matching.inserts.add(currentIds(d)))
    matching
  }
}
