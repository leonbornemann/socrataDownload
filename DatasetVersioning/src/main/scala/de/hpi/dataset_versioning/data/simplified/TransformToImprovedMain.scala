package de.hpi.dataset_versioning.data.simplified

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.io.IOService

object TransformToImprovedMain extends App {
  IOService.socrataDir = args(0)
  val id = args(1)
  val versions = IOService.getSortedMinimalUmcompressedVersions
    .filter(d => IOService.getMinimalUncompressedVersionDir(d).listFiles().exists(f => {
      f.getName == IOService.jsonFilenameFromID(id)
    }))
  println(versions)
  for (version <- versions) {
    val ds = IOService.tryLoadDataset(DatasetInstance(id,version),true)
    val improved = ds.toImproved
    val outDir = new File(args(2) + s"/${IOService.dateTimeFormatter.format(version)}")
    outDir.mkdir()
    val outFile = new File(outDir.getAbsolutePath + s"/$id.json?")
    improved.toJsonFile(outFile)
    val improvedParsed = RelationalDataset.fromJsonFile(outFile.getAbsolutePath)
    println()
  }
}
