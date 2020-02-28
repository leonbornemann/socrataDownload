package de.hpi.dataset_versioning.io.diff

import java.io.{File, FileInputStream, FileOutputStream, PrintWriter}
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.zip.{ZipEntry, ZipInputStream}

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io
import de.hpi.dataset_versioning.io.IOService
import org.joda.time.Days

import scala.io.Source
import scala.reflect.io.Directory
import scala.sys.process._

class DiffCalculator(/*workingDirectory: File,*/var diffDirectory:File=null) extends StrictLogging{

  def getLines(file: File) = Source.fromFile(file).getLines().toSet

  def isDiffFile(f: File): Boolean = f.getName.endsWith(".diff")

  def isDataFile(f: File): Boolean = f.getName.endsWith(".json?")

  def diffToOriginalName(diffFilename: String) = diffFilename.substring(0,diffFilename.lastIndexOf('.'))

  /*def recreateFromDiff(previous: File, target:File) = {
    val deleted = getLines(new File(DELETED))
    val created = getLines(new File(CREATED))
    //copy all created files:
    diffs
      .listFiles()
      .foreach(f => {
        if(isDiffFile(f)){
          val diffFilename = f.getName
          val originalName = diffToOriginalName(diffFilename)
          val originalFilepath = FROM.getAbsolutePath + "/" + originalName
          val toExecute = s"patch -o ${target.getAbsolutePath + "/" + originalName} $originalFilepath ${f.getAbsolutePath}"
          toExecute! //TODO: deal with rejects!
          //TODO assert file equality
        } else if(isDataFile(f)){
          val toExecute = s"cp ${f.getAbsolutePath} ${target.getAbsolutePath}"
          toExecute!
        } else{
          println("skipping meta file " +f.getName)
        }
      })
  }*/

  /*def clearToAndFrom() = {
    TO.listFiles().foreach(_.delete())
    FROM.listFiles().foreach(_.delete())
  }*/


  /*val TO = new File(workingDirectory.getAbsolutePath + File.separator + "to" + File.separator)
  val FROM = new File(workingDirectory.getAbsolutePath + File.separator + "from" + File.separator)
  if(diffs==null)
    diffs = new File(workingDirectory.getAbsolutePath + File.separator + "diffFiles" + File.separator)
  val DELETED = diffs.getAbsolutePath + "/deleted.txt"
  val CREATED = diffs.getAbsolutePath + "/created.txt"
  TO.mkdirs()
  FROM.mkdirs()
  diffs.mkdir()*/

  def deleteUnmeaningfulDiffs() = {
    diffDirectory.listFiles()
      .filter(_.length()==0)
      .foreach(_.delete())
  }

  def calculateAllDiffsFromUncompressed(from: LocalDate, to: LocalDate,deleteUncompressed:Boolean) = {
    val filesFrom = IOService.extractDataToWorkingDir(from)
    val filesTo = IOService.extractDataToWorkingDir(to)
    val namesFrom = filesFrom.map(f => (f.getName,f)).toMap
    val namesTo = filesTo.map(f => (f.getName,f)).toMap
    //diffs and deleted files:
    assert(diffDirectory.exists() && diffDirectory.listFiles.size==0)
    val deleted = new PrintWriter(diffDirectory.getAbsolutePath + "/deleted.meta")
    val created = new PrintWriter(diffDirectory.getAbsolutePath + "/created.meta")
    namesFrom.values.foreach(f => {
      if(namesTo.contains(f.getName)){
        val f2 = namesTo(f.getName)
        val targetFile = new File(diffDirectory.getAbsolutePath + s"/${f.getName}.diff")
        val toExecute = s"diff ${f.getAbsolutePath} ${f2.getAbsolutePath}"
        val targetFilePath = targetFile.getAbsolutePath
        (toExecute #> new File(targetFilePath)).!
      } else{
        deleted.println(f.getName)
      }
    })
    deleted.close()
    //newly created files:
    namesTo.keySet.diff(namesFrom.keySet).foreach(f => {
      val file = namesTo(f)
      created.println(f)
      val toExecute = s"cp ${file.getAbsolutePath} ${diffDirectory.getAbsolutePath}"
      toExecute!
    })
    created.close()
    deleteUnmeaningfulDiffs()
    //zip the resulting directory:
    val toExecute = s"zip -q -r ${IOService.DIFF_DIR + "/" + diffDirectory.getName}.zip $diffDirectory"
    toExecute!;
    if(deleteUncompressed)
      new Directory(diffDirectory).deleteRecursively()
  }

  def calculateDiff(version:LocalDate,deleteUncompressed:Boolean=true) = {
    logger.trace("calculating diff for {}",version)
    IOService.extractDataToWorkingDir(version)
    val previousVersion = version.minusDays(1)
    assert(IOService.versionExists(previousVersion))
    IOService.extractDataToWorkingDir(previousVersion)
    calculateAllDiffsFromUncompressed(previousVersion,version,deleteUncompressed)
  }


}
