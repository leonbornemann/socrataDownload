package de.hpi.dataset_versioning.experiment.example_query_imputation.join

import java.time.LocalDate

import de.hpi.dataset_versioning.experiment.example_query_imputation.QueryRelationshipDiscoverer
import de.hpi.dataset_versioning.io.IOService

object JoinFinderMain extends App {
  IOService.socrataDir = args(0)
  val version = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  IOService.cacheMetadata(version)
  val projectionFinder = new QueryRelationshipDiscoverer(version)
  projectionFinder.findJoins(version)
}
