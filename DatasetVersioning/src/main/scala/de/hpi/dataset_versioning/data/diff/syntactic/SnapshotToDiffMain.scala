package de.hpi.dataset_versioning.data.diff.syntactic

import java.io.File

import de.hpi.dataset_versioning.io.IOService

object SnapshotToDiffMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
  val transformer = new DiffManager(7)
  transformer.replaceAllNonCheckPointsWithDiffs(new File(args(1)))

}
