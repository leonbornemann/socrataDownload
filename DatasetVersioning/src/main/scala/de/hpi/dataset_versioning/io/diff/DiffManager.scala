package de.hpi.dataset_versioning.io.diff

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.{IOService, IOUtil}

class DiffManager(daysBetweenCheckpoints:Int=7) extends StrictLogging{

  def replaceAllNonCheckPointsWithDiffs(tmpDirectory:File) = {
    val versions = IOService.getSortedDatalakeVersions()
    for(i <- 1 until versions.size) {
      val version = versions(i)
      if(!isCheckpoint(i) && !IOService.compressedDiffExists(version)){
        logger.debug(s"Starting replacement of $version")
        logger.debug(s"Calculating Diff")
        calculateDiff(version)
        logger.debug(s"Testing Snapshot Restore in temporary Directory")
        restoreSnapshotFromDiff(version,Some(tmpDirectory))
        IOService.extractDataToWorkingDir(version)
        val uncompressedDir = IOService.getUncompressedDataDir(version)
        if(IOUtil.dirEquals(tmpDirectory,uncompressedDir)){
          logger.debug(s"Snapshot Restore successful - deleting zipped files")
          IOService.getCompressedDataFile(version).delete()
        } else{
          throw new AssertionError(s"Restored Directory ${tmpDirectory.getAbsolutePath} contents do not match original ($uncompressedDir) - aborting")
        }
        if(IOService.uncompressedSnapshotExists(version.minusDays(2))){
          IOService.clearUncompressedSnapshot(version.minusDays(2))
          IOService.clearUncompressedDiff(version.minusDays(2))
        }
        if(IOService.uncompressedSnapshotExists(version.minusDays(3))){
          //happens if we passed a checkpoint
          IOService.clearUncompressedSnapshot(version.minusDays(3))
          IOService.clearUncompressedDiff(version.minusDays(3))
        }
        logger.debug(s"Cleaning up temporary Directory")
        IOUtil.clearDirectoryContent(tmpDirectory)
      }
    }
  }


  def isCheckpoint(i: Int): Boolean = i % daysBetweenCheckpoints==0
  val diffCalculator = new DiffCalculator

  def calculateDiff(version:LocalDate) = {
    if(!IOService.compressedDiffExists(version)){
      val diffDir = IOService.getUncompressedDiffDir(version)
      if(diffDir.exists()){
        IOUtil.clearDirectoryContent(diffDir)
      }
      diffDir.mkdirs()
      diffCalculator.calculateDiff(version)
    } else{
      logger.debug(s"Skipping diff for version $version because it already exists")
    }
  }

  def restoreSnapshotFromDiff(version:LocalDate,targetDir:Option[File] = None) = {
    if(IOService.compressedSnapshotExists(version)){
      logger.trace(s"Skipping restore of ${version} because it already exists")
    } else if(IOService.getUncompressedDataDir(version).exists() && !targetDir.isDefined){
      logger.trace(s"Not restoring ${version} from Diff because uncompressed Snapshot exists for it - Compressed Snapshot will be created from uncompressed File.")
      IOService.compressDataFromWorkingDir(version)
    } else{
      assert(IOService.compressedDiffExists(version))
      IOService.extractDataToWorkingDir(version.minusDays(1))
      IOService.extractDiffToWorkingDir(version)
      if(!targetDir.isDefined)
        IOService.getUncompressedDataDir(version).mkdirs()
      diffCalculator.recreateFromDiff(version,targetDir)
    }
  }

  def calculateAllDiffs() = {
    val versions = IOService.getSortedDatalakeVersions()
    //start at 1 because origin must be kept anyway
    for(i <- 0 until versions.size) {
      if(!isCheckpoint(i)){
        calculateDiff(versions(i))
      }
    }
  }
}
