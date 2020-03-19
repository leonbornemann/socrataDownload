package de.hpi.dataset_versioning.experiment

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.data.parser.JsonDataParser
import de.hpi.dataset_versioning.io.IOService

object Oneshot extends App {
  IOService.socrataDir = args(0)
  private val date: LocalDate = LocalDate.parse(args(1), IOService.dateTimeFormatter)
  IOService.cacheMetadata(date)
  private val id1 = "53jv-eiur"
  private val id2 = "pqbh-p6xe"
  private val dir = "/home/leon/Desktop/tmp/" + id1 + "_" + id2 + "/"
  new File(dir).mkdir()
  IOService.tryLoadDataset(id1,date)
    .exportToCSV(new File(dir+id1+".csv"))
  IOService.tryLoadDataset(id2,date)
    .exportToCSV(new File(dir + id2 + ".csv"))
}
