package de.hpi.dataset_versioning.data.metadata.custom.joinability.`export`

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService

object LSHEnsembleDomainExportMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
  val version = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val outFile = new File(args(2))
  new LSHEnsembleDomainExporter().export(version,outFile)

}
