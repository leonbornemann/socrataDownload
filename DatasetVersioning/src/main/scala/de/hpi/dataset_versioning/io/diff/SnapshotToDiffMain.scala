package de.hpi.dataset_versioning.io.diff

import java.io.File

import de.hpi.dataset_versioning.io.{IOService, IOUtil}
import de.hpi.dataset_versioning.io.diff.DiffCreationMain.args

object SnapshotToDiffMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
  val transformer = new DiffManager(7)
  transformer.replaceAllNonCheckPointsWithDiffs(new File(args(1)))
}
