package de.hpi.dataset_versioning.data.metadata.custom.joinability.`export`

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.matching.DatasetInstance

import scala.collection.mutable.ArrayBuffer

class LSHEnsembleDomainExporter() extends StrictLogging{


  def loadChangedDatasetsPerVersion():Map[LocalDate,Set[String]] = ???

  def export(startVersion: LocalDate, endVersion:LocalDate, outDir: File) = {
    //first snapshot full:
    val datasetVersionHistory = loadChangedDatasetsPerVersion()
    val files = IOService.extractDataToWorkingDir(startVersion)
    val writer = new PrintWriter(outDir.getAbsolutePath + s"/${IOService.dateTimeFormatter.format(startVersion)}")
    var count = 0
    files.foreach(f => {
      if(count%1000==0) logger.debug(s"Finsihed domain export of $count datasets")
      val ds = IOService.tryLoadDataset(new DatasetInstance(IOService.filenameToID(f),startVersion))
      ds.exportColumns(writer)
      count+=1
    })
    writer.close()
    //oter snapshots:
    val allVersions = IOService.getSortedDatalakeVersions()
    val versionsToExport = ArrayBuffer[LocalDate]() ++ allVersions.slice(allVersions.indexOf(startVersion)+1,allVersions.indexOf(endVersion)+1)
    val batchesBetweenCheckpoints = ArrayBuffer[ArrayBuffer[LocalDate]]()
    val checkpoints = IOService.getCheckpoints()
    checkpoints.zipWithIndex.foreach{ case(cp,i) => {
      assert(cp.toEpochDay <= versionsToExport(0).toEpochDay)
      if(i==checkpoints.size-1) {
        if(!versionsToExport.isEmpty)
          batchesBetweenCheckpoints += versionsToExport
      } else{
        val nextCP = checkpoints(i+1)
        batchesBetweenCheckpoints += removeWhile(versionsToExport,snapshot => snapshot.toEpochDay >= cp.toEpochDay && snapshot.toEpochDay < nextCP.toEpochDay)
      }

    }}
    //now we have all batches, the first of which is a snapshot
    batchesBetweenCheckpoints.foreach(batch => {

    })
    versionsToExport.foreach(v => {
      val ids = datasetVersionHistory.getOrElse(v,Set())
      if(!ids.isEmpty){
        //TODO: load snapshot and get files
      }
    })


  }
  def removeWhile(list:ArrayBuffer[LocalDate],f:LocalDate => Boolean):ArrayBuffer[LocalDate] = {
    val newList = list.takeWhile(f)
    list.dropWhile(f)
    newList
  }
}
