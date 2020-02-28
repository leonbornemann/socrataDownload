package de.hpi.dataset_versioning.io.diff

import java.io.File

import de.hpi.dataset_versioning.io.IOService

object SnapshotToDiffMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
  val diffWorkingDir = new File(args(1))
  val transformer = new SnapshotToDiffTransformer(diffWorkingDir,7)
  transformer.transformAll()
}
