package de.hpi.dataset_versioning.io

import de.hpi.dataset_versioning.io.diff.SnapshotToDiffMain.args

object StatusReportMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
}
