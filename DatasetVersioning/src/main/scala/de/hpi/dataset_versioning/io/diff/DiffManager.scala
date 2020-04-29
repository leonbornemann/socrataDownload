package de.hpi.dataset_versioning.io.diff

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.custom.joinability.`export`.SnapshotDiff
import de.hpi.dataset_versioning.io.{IOService, IOUtil}

class DiffManager(daysBetweenCheckpoints:Int=7) extends StrictLogging{

  def replaceAllNonCheckPointsWithDiffs(tmpDirectory:File) = {
    val versions = IOService.getSortedDatalakeVersions()
    val actions = (0 until versions.size).map(i => {
      if(!isCheckpoint(i) && IOService.compressedDiffExists(versions(i)))
        (versions(i),"Keep compressed Diff as is")
      else if(!isCheckpoint(i) && !IOService.compressedDiffExists(versions(i)))
        (versions(i),"Replace with compressed Diff")
      else
        (versions(i),"Keep as Checkpoint")
    })
    logger.debug("DiffManager is about to perform the following actions:")
    actions.groupBy(a =>a._2)
        .toSeq
        .sortBy(_._2.head._1.toEpochDay)
        .foreach{ case(action,versions) => logger.debug(s"$action: ${versions.map(_._1)}")}
    logger.debug("Enter y to continue, anything else to exit")
    val input = scala.io.StdIn.readLine()
    if(input.toLowerCase() == "y") {
      for(i <- 1 until versions.size) {
        val version = versions(i)
        if(!isCheckpoint(i) && !IOService.compressedDiffExists(version)){
          logger.debug(s"Starting replacement of $version")
          logger.debug(s"Calculating Diff")
          calculateDiff(version)
          logger.debug(s"Testing Snapshot Restore in temporary Directory")
          restoreFullSnapshotFromDiff(version,Some(tmpDirectory))
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
    } else{
      logger.debug("terminating")
    }
  }


  def isCheckpoint(i: Int): Boolean = i % daysBetweenCheckpoints==0
  val diffCalculator = new DiffCalculator

  def calculateDiff(version:LocalDate,deleteUncompressed:Boolean = true,restorePreviousSnapshotIfNecessary:Boolean=true) = {
    if(!IOService.compressedDiffExists(version)){
      val diffDir = IOService.getUncompressedDiffDir(version)
      if(diffDir.exists()){
        IOUtil.clearDirectoryContent(diffDir)
      }
      diffDir.mkdirs()
      diffCalculator.calculateDiff(version,deleteUncompressed,restorePreviousSnapshotIfNecessary)
    } else{
      logger.debug(s"Skipping diff for version $version because it already exists")
    }
  }

  def restoreMinimalSnapshot(version:LocalDate) = {
    if(IOService.minimalUncompressedVersionDirExists(version)){
      logger.debug(s"skipping minimal snapshot restore of version $version because it already exists")
    } else {
      logger.debug(s"beginning minimal snapshot restore of version $version")
      IOService.extractDiffToWorkingDir(version)
      val diff = new SnapshotDiff(version, IOService.getUncompressedDiffDir(version))
      val dstDir = IOService.getMinimalUncompressedVersionDir(version)
      //copy created files
      diff.createdDatasetFiles.foreach(f => {
        val dst = new File(dstDir + "/" + f.getName)
        java.nio.file.Files.copy(f.toPath, dst.toPath)
      })
      logger.debug("finished copying created files")
      //TODO: create a list of deleted files (?) - would be an integrity check only
      //patch changed files
      diff.diffFiles.foreach(diffFile => {
        //find latest version:
        val srcFile = getLatestVersionBefore(version, IOService.filenameToID(diffFile)) //TODO: weird output here+ IOService.filenameToID(diffFile)
        diffCalculator.patchFile(dstDir, diffFile, srcFile.getName, srcFile.getAbsolutePath)
      })
      logger.debug(s"Finished minimal snapshot restore for version $version")
    }
    IOService.getMinimalUncompressedVersionDir(version).listFiles
  }

  def getLatestVersionBefore(version: LocalDate, id: String) = {
    val versions = IOService.getSortedMinimalUmcompressedVersions
      .filter(v => {
      val containsFileID = IOService.getMinimalUncompressedVersionDir(v)
        .listFiles()
        .map(IOService.filenameToID(_))
        .contains(id)
      v.toEpochDay < version.toEpochDay && containsFileID
    })
    new File(s"${IOService.getMinimalUncompressedVersionDir(versions.last).getAbsolutePath}/$id.json?")
  }

  def restoreFullSnapshotFromDiff(version:LocalDate, targetDir:Option[File] = None,recursivelyRestoreSnapshots:Boolean = false):Unit = {
    if(IOService.compressedSnapshotExists(version)){
      logger.trace(s"Skipping restore of ${version} because it already exists")
    } else if(IOService.uncompressedSnapshotExists(version) && !targetDir.isDefined){
      logger.trace(s"Not restoring ${version} from Diff because uncompressed Snapshot exists for it - Compressed Snapshot will be created from uncompressed File.")
      IOService.compressDataFromWorkingDir(version)
    } else{
      assert(IOService.compressedDiffExists(version))
      if(!recursivelyRestoreSnapshots && !IOService.snapshotExists(version.minusDays(1))){
        throw new AssertionError(s"no snapshot available for ${version.minusDays(1)}")
      } else if(IOService.snapshotExists(version.minusDays(1))) {
        IOService.extractDataToWorkingDir(version.minusDays(1))
        IOService.extractDiffToWorkingDir(version)
        if(!targetDir.isDefined)
          IOService.getUncompressedDataDir(version).mkdirs()
        diffCalculator.recreateFromDiff(version,targetDir)
      } else{
        assert(recursivelyRestoreSnapshots)
        logger.debug(s"recursively restoring ${version.minusDays(1)}")
        restoreFullSnapshotFromDiff(version.minusDays(1),recursivelyRestoreSnapshots=true)
        //TODO: fix code copy paste?
        IOService.extractDataToWorkingDir(version.minusDays(1))
        IOService.extractDiffToWorkingDir(version)
        if(!targetDir.isDefined)
          IOService.getUncompressedDataDir(version).mkdirs()
        diffCalculator.recreateFromDiff(version,targetDir)
        IOService.clearUncompressedSnapshot(version.minusDays(1))
        IOService.saveDeleteCompressedDataFile(version.minusDays(1))
      }
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
