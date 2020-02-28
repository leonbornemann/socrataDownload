package de.hpi.dataset_versioning.matching

import scala.collection.mutable

class Matching() {

  val deletes = mutable.HashSet[DatasetInstance]()
  val matchings = mutable.HashMap[DatasetInstance,DatasetInstance]()
  val inserts = mutable.HashSet[DatasetInstance]()

}
