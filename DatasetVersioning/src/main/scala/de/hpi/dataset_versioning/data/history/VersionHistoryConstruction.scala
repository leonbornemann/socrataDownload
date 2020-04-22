package de.hpi.dataset_versioning.data.history

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class VersionHistoryConstruction() {

  def getSnapshotModifications(diffFiles: Set[File]) = {
    val createdDatasets = mutable.ArrayBuffer[String]()
    val shouldBeCreated = mutable.ArrayBuffer[String]()
    val deletedDatasets = mutable.ArrayBuffer[String]()
    val changedDatasets = mutable.ArrayBuffer[String]()
    diffFiles.foreach(f => {
      if(f.getName =="created.meta"){
        shouldBeCreated ++= Source.fromFile(f).getLines().toSeq.map(_.split(".")(0))
      } else if(f.getName== "deleted.meta")
        deletedDatasets ++= Source.fromFile(f).getLines().toSeq.map(_.split(".")(0))
      else if(f.getName.endsWith(".json")){
        createdDatasets += IOService.filenameToID(f)
      } else{
        assert(f.getName.endsWith(".diff"))
        changedDatasets += f.getName.split(".")(0)
      }
    })
    assert(createdDatasets.toSet == shouldBeCreated.toSet)
    assert(createdDatasets.intersect(changedDatasets).isEmpty)
    (createdDatasets,changedDatasets,deletedDatasets)
  }

  def constructVersionHistory() = {
    val versions = IOService.getSortedDatalakeVersions()
    //initialize:
    val initialFiles = IOService.extractDataToWorkingDir(versions(0))
    val idToVersions = scala.collection.mutable.HashMap[String,DatasetVersionHistory]()
    initialFiles.foreach(f => {
      val id = IOService.filenameToID(f)
      val history = idToVersions.getOrElseUpdate(id, new DatasetVersionHistory(id))
      history.versionsWithChanges += versions(0)
    })
    for(i <- 1 until versions.size){
      val curVersion = versions(0)
      val diffFiles = IOService.extractDiffToWorkingDir(curVersion)
      val (createdDatasets,changedDatasets,deletedDatasets) = getSnapshotModifications(diffFiles)
      (createdDatasets ++changedDatasets).foreach(id => {
        val history = idToVersions.getOrElseUpdate(id,new DatasetVersionHistory(id))
        history.versionsWithChanges += curVersion
      })
      deletedDatasets.foreach(id => {
        val history = idToVersions.getOrElseUpdate(id,new DatasetVersionHistory(id))
        history.deletions += curVersion
      })
      val versionHistoryFile = IOService.getVersionHistoryFile()
      ???
      //TODO: make this json and manage to serialize/deserialize local dates correctly
    }
  }


}