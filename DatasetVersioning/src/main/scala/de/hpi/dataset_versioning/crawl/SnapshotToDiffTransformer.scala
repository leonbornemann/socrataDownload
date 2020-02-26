package de.hpi.dataset_versioning.crawl

import java.io.File

class SnapshotToDiffTransformer(dataDir:File,workingDir:File,daysBetweenCheckpoints:Int=7) {

  val diffWorkingDir = new File(workingDir.getAbsolutePath + "/diffWorkingDir/")
  diffWorkingDir.mkdirs()
  val testDir = new File(workingDir.getAbsolutePath + "/tmp/")
  testDir.mkdirs()
  def isCheckpoint(i: Int): Boolean = i % daysBetweenCheckpoints==0

  def replaceSnapshotWithDiff(toReplace: File, previous: File) = {
    val diffDir = new File(toReplace.getParent + "/" + toReplace.getName + "_diff")
    val diffCalculator = new DiffCalculator(diffWorkingDir,diffDir)
    diffCalculator.calculateDiff(toReplace,previous)
    //assert that we can recreate TO from the diff files
    diffCalculator.recreateFromDiff(previous,testDir)
    //diffCalculator.clearToAndFrom()
  }

  def transformAll() = {
    val filesSorted = dataDir
      .listFiles()
      .toIndexedSeq
      .filter(_.getName.endsWith(".zip"))
      .sorted
    //start at 1 because origin must be kept anyway
    for(i <- 1 until filesSorted.size) {
      if(!isCheckpoint(i)){
        replaceSnapshotWithDiff(filesSorted(i),filesSorted(i-1))
      }
    }
  }

}
