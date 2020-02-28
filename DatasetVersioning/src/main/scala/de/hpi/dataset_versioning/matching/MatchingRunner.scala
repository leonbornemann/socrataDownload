package de.hpi.dataset_versioning.matching

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

import scala.collection.immutable

class MatchingRunner() extends StrictLogging{

  def evaluateMatchings = {
    val dataLakeVersions = IOService.getSortedDatalakeVersions()
    executeMatchingBetweenSnapshots(dataLakeVersions(0),dataLakeVersions(dataLakeVersions.size-2))
    //matchAll(dataLakeVersions)
  }

  private def matchAll(zippedDatalakeFiles: immutable.IndexedSeq[LocalDate]) = {
    (0 until zippedDatalakeFiles.size - 1).foreach(i => {
      executeMatchingBetweenSnapshots(zippedDatalakeFiles(i), zippedDatalakeFiles(i + 1))
    })
  }

  private def executeMatchingBetweenSnapshots(previous:LocalDate, current:LocalDate) = {
    logger.debug(s"Executing matching between ${previous} and ${current}")
    //TODO: extract zips to directory
    //create matching
    val previousDatasets = IOService.extractDataToWorkingDir(previous)
      .map(f => new DatasetInstance(IOService.filenameToID(f), previous))
      .toSet
    val currentDatasets = IOService.extractDataToWorkingDir(current)
      .map(f => new DatasetInstance(IOService.filenameToID(f), current))
      .toSet
    val groundTruth = new GroundTruthMatching()
      .matchDatasets(previousDatasets, currentDatasets)
    val resourceNameBaseline = new ResourceNameEqualityMatching()
      .matchDatasets(previousDatasets, currentDatasets)
    val eval = new MatchingEvaluation(resourceNameBaseline.matchings.toMap, groundTruth.matchings.toMap) // TODO: this does not evaluate inserts or deletes
    logger.debug(s"${eval.confusionMatrix.accuracy}")
    logger.debug(s"${eval.confusionMatrix}")
  }

  def getGroundTruthMatching(previous:LocalDate, current:LocalDate) = {
    val previousDatasets = IOService.extractDataToWorkingDir(previous)
      .map(f => new DatasetInstance(IOService.filenameToID(f), previous))
      .toSet
    val currentDatasets = IOService.extractDataToWorkingDir(current)
      .map(f => new DatasetInstance(IOService.filenameToID(f), current))
      .toSet
    val groundTruth = new GroundTruthMatching()
      .matchDatasets(previousDatasets, currentDatasets)
    groundTruth
  }
}
