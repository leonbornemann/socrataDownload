package de.hpi.dataset_versioning.data.history

import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

object VersionHistoryAnalysisMain extends App {
  IOService.socrataDir = args(0)
  val versionHistoryFile = IOService.getVersionHistoryFile()
  val list = Source.fromFile(versionHistoryFile)
    .getLines()
    .toSeq
    .map(s => DatasetVersionHistory.fromJsonString(s))
  val a = list.map(_.versionsWithChanges.size)
  val histogram = a.groupBy(identity)
    .mapValues(_.size)
  println("Num_versions,num_histories")
  histogram.toIndexedSeq.sortBy(_._1)
    .foreach(t => println(t._1 + "," +t._2))
  val b = list//.filter(_.versionsWithChanges.size>9)
      .flatMap(_.versionsWithChanges)
      .groupBy(identity)
      .mapValues(_.size)
  println("Date,num_new_versions")
  b.toIndexedSeq
      .sortBy(_._1.toEpochDay)
      .foreach(t => println(t._1 + "," +t._2))
  println()
}
