package de.hpi.dataset_versioning.crawl.matching

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging

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
      .map(f => new Dataset(IOService.filenameToID(f), previous))
      .toSet
    val currentDatasets = IOService.extractDataToWorkingDir(current)
      .map(f => new Dataset(IOService.filenameToID(f), current))
      .toSet
    val groundTruth = new GroundTruthMatching()
      .matchDatasets(previousDatasets, currentDatasets)
    val resourceNameBaseline = new ResourceNameEqualityMatching()
      .matchDatasets(previousDatasets, currentDatasets)
    val eval = new MatchingEvaluation(resourceNameBaseline.toMap, groundTruth.toMap)
    logger.debug(s"${eval.confusionMatrix.accuracy}")
    logger.debug(s"${eval.confusionMatrix}")
  }
}
