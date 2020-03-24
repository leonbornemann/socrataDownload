package de.hpi.dataset_versioning.experiment

import de.hpi.dataset_versioning.data.metadata.custom.IdentifierMapping
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.io.StatusReportMain.args

object IDMapperMain extends App {

  IOService.socrataDir = args(0)
  val version = IOService.getSortedDatalakeVersions()(0)
  IdentifierMapping.createDatasetIDMapping(version)
  IdentifierMapping.createColumnIDMapping(version)
}
