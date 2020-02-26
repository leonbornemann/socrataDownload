package de.hpi.dataset_versioning.crawl.matching
import java.io.File

import scala.collection.mutable

class GroundTruthMatching extends MatchingAlgorithm {

  def isDatasetFile(str: String): Boolean = str.endsWith(".json")

  override def matchDatasets(previous: Set[Dataset], current: Set[Dataset]) = {
    val currentIds = current.map(d => (d.id,d))
        .toMap
    previous.foreach( d => {
      if(currentIds.contains(d.id)){
        matching.put(d,currentIds(d.id))
      } else{

      }
    })
    matching
  }
}
