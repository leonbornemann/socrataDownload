package de.hpi.dataset_versioning.crawl

import java.io.{File, FileInputStream, FileOutputStream, PrintWriter}
import java.util.zip.{ZipEntry, ZipInputStream}

import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.io.Source
import sys.process._

class DiffCalculator(workingDirectory: File,var diffs:File=null) extends StrictLogging{

  def getLines(file: File) = Source.fromFile(file).getLines().toSet

  def isDiffFile(f: File): Boolean = f.getName.endsWith(".diff")

  def isDataFile(f: File): Boolean = f.getName.endsWith(".json?")

  def diffToOriginalName(diffFilename: String) = diffFilename.substring(0,diffFilename.lastIndexOf('.'))

  def recreateFromDiff(previous: File, target:File) = {
    val deleted = getLines(new File(DELETED))
    val created = getLines(new File(CREATED))
    //copy all created files:
    diffs
      .listFiles()
      .foreach(f => {
        if(isDiffFile(f)){
          //TODO: apply patch command
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
  }

  def clearToAndFrom() = {
    TO.listFiles().foreach(_.delete())
    FROM.listFiles().foreach(_.delete())
  }


  val TO = new File(workingDirectory.getAbsolutePath + File.separator + "to" + File.separator)
  val FROM = new File(workingDirectory.getAbsolutePath + File.separator + "from" + File.separator)
  if(diffs==null)
    diffs = new File(workingDirectory.getAbsolutePath + File.separator + "diffFiles" + File.separator)
  val DELETED = diffs.getAbsolutePath + "/deleted.txt"
  val CREATED = diffs.getAbsolutePath + "/created.txt"
  TO.mkdirs()
  FROM.mkdirs()
  diffs.mkdir()

  def deleteUnmeaningfulDiffs() = {
    diffs.listFiles()
      .filter(_.length()==0)
      .foreach(_.delete())
  }

  def calculateAllDiffsFromUncompressed(uncompressedFrom: File, uncompressedTo: File) = {
    val namesFrom = uncompressedFrom.listFiles().map(f => (f.getName,f)).toMap
    val namesTo = uncompressedTo.listFiles().map(f => (f.getName,f)).toMap
    //diffs and deleted files:
    val deleted = new PrintWriter(DELETED)
    namesFrom.values.foreach(f => {
      if(namesTo.contains(f.getName)){
        val f2 = namesTo(f.getName)
        val targetFile = new File(diffs.getAbsolutePath + s"/${f.getName}.diff")
        val toExecute = s"diff ${f.getAbsolutePath} ${f2.getAbsolutePath}"
        val targetFilePath = targetFile.getAbsolutePath
        (toExecute #> new File(targetFilePath)).!
      } else{
        deleted.println(f.getName)
      }
    })
    deleted.close()
    //newly created files:
    val created = new PrintWriter(CREATED)
    namesTo.keySet.diff(namesFrom.keySet).foreach(f => {
      val file = namesTo(f)
      created.println(f)
      val toExecute = s"cp ${file.getAbsolutePath} ${diffs.getAbsolutePath}"
      toExecute!
    })
    created.close()
    deleteUnmeaningfulDiffs()
  }

  def calculateDiff(from: File, to: File) = {
    logger.trace("calculating diff from {} to {}",from.getAbsolutePath,to.getAbsolutePath)
    //extractAll(from, FROM)
    //extractAll(to, TO)
    calculateAllDiffsFromUncompressed(FROM,TO)
  }


  def extractAll(from: File, subDirectory: File) = {
    val buffer = new Array[Byte](1024)
    val zis: ZipInputStream = new ZipInputStream(new FileInputStream(from))
    var ze: ZipEntry = zis.getNextEntry();
    while(ze !=null){
      if(!ze.isDirectory){
        val nameTokens = ze.getName.split("/")
        val name = nameTokens(nameTokens.size-1)
        val newFile = new File(subDirectory.getAbsolutePath + File.separator + name);
        val fos = new FileOutputStream(newFile);
        var len: Int = zis.read(buffer);
        while (len > 0) {
          fos.write(buffer, 0, len)
          len = zis.read(buffer)
        }
        fos.close()
      }
      ze = zis.getNextEntry
    }
    zis.close()
  }

  def calculateAllDiffs(directory:String) = {
    val files = new File(directory)
      .listFiles()
      .toSeq
      .filter(_.getName.endsWith(".zip"))
      .sorted
    files.slice(0,files.size-1).zip(files.slice(1,files.size))
      .foreach{case (from,to) => calculateDiff(from,to)}
  }

}
