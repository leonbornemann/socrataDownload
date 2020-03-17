package de.hpi.dataset_versioning.experiment.example_query_imputation

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.experiment.JoinabilityGraphExplorer
import de.hpi.dataset_versioning.io.IOService

object ProjectionFinderMain extends App {
  IOService.socrataDir = args(0)
  val version = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  IOService.cacheMetadata(version)
  val projectionFinder = new ProjectionFinder(version)
  projectionFinder.find(version)
}
