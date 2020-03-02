package de.hpi.dataset_versioning.io.diff

import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService

object SnapshotRestoreMain extends App {
  //TODO: arguments whether we want it compressed or uncompressed?
  val socrataDir = args(0)
  IOService.socrataDir = socrataDir
  IOService.printSummary()
  val version = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val transformer = new DiffManager(7)
  transformer.restoreSnapshotFromDiff(version)
}
