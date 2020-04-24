package de.hpi.dataset_versioning.data.metadata.custom.joinability.`export`

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService

object LSHEnsembleDomainExportMain extends App {
  IOService.socrataDir = args(0)
  IOService.printSummary()
  val startVersion = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val endVersion = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val outDir = new File(args(2))
  new LSHEnsembleDomainExporter().export(startVersion,endVersion,outDir)

}
