package de.hpi.dataset_versioning.io.diff

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

class SnapshotToDiffTransformer(workingDir:File,daysBetweenCheckpoints:Int=7) extends StrictLogging{

  val diffWorkingDir = new File(workingDir.getAbsolutePath + "/diffWorkingDir/")
  diffWorkingDir.mkdirs()
  val testDir = new File(workingDir.getAbsolutePath + "/tmp/")
  testDir.mkdirs()
  def isCheckpoint(i: Int): Boolean = i % daysBetweenCheckpoints==0

  def replaceSnapshotWithDiff(version:LocalDate) = {
    if(!IOService.diffExists(version)){
      val diffDir = IOService.getUncompressedDiffDirForDate(version)
      assert(!diffDir.exists())
      diffDir.mkdirs()
      val diffCalculator = new DiffCalculator(diffDir)
      diffCalculator.calculateDiff(version)
      //assert that we can recreate TO from the diff files
      //diffCalculator.recreateFromDiff(previous,testDir)
      //diffCalculator.clearToAndFrom()
    } else{
      logger.debug(s"Skipping diff for version $version because it already exists")
    }

  }

  def transformAll() = {
    val versions = IOService.getSortedDatalakeVersions()
      .slice(0,10) //to not break stuff
    //start at 1 because origin must be kept anyway
    for(i <- 0 until versions.size) {
      if(!isCheckpoint(i)){
        replaceSnapshotWithDiff(versions(i))
      }
    }
  }
}
