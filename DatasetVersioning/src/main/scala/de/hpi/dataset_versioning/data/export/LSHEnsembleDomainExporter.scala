package de.hpi.dataset_versioning.data.`export`

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.matching.DatasetInstance

class LSHEnsembleDomainExporter() extends StrictLogging{
  def export(version: LocalDate, outFile: File) = {
    val files = IOService.extractDataToWorkingDir(version)
    val writer = new PrintWriter(outFile)
    var count = 0
    files.foreach(f => {
      if(count%1000==0) logger.debug(s"Finsihed domain export of $count datasets")
      val ds = IOService.tryLoadDataset(new DatasetInstance(IOService.filenameToID(f),version))
      ds.exportColumns(writer)
      count+=1
    })
    writer.close()
  }
}
