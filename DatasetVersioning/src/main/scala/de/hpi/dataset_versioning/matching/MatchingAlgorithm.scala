package de.hpi.dataset_versioning.matching

import java.io.File

import scala.collection.mutable

trait MatchingAlgorithm {

  val matching = new Matching()

  def matchDatasets(previous:Set[DatasetInstance], current:Set[DatasetInstance]):Matching

}
