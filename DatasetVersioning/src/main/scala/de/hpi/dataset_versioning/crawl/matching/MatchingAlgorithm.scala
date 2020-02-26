package de.hpi.dataset_versioning.crawl.matching

import java.io.File

import scala.collection.mutable

trait MatchingAlgorithm {

  val matching = mutable.HashMap[Dataset,Dataset]()

  def matchDatasets(previous:Set[Dataset],current:Set[Dataset]):mutable.HashMap[Dataset,Dataset]

}
