package de.hpi.dataset_versioning.crawl

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.crawl.DiffCalculatorMain.directory

import scala.collection.mutable
import sys.process._

class DiffCalculator(dataDirectory: File, diffDir: File) extends StrictLogging{

  val TO = new File(diffDir.getAbsolutePath + File.separator + "to" + File.separator)
  val FROM = new File(diffDir.getAbsolutePath + File.separator + "from" + File.separator)
  val diffs = new File(diffDir.getAbsolutePath + File.separator + "diffFiles" + File.separator)
  TO.mkdirs()
  FROM.mkdirs()
  diffs.mkdir()

  def deleteUnmeaningfulDiffs() = {
    diffs.listFiles()
      .filter(_.length()==0)
      .foreach(_.delete())
  }

  def calculateAllDiffsFromUncompressed(uncompressedFrom: File, uncompressedTo: File) = {
    val namesFrom = uncompressedFrom.listFiles()
    val namesTo = uncompressedTo.listFiles().map(f => (f.getName,f)).toMap
    namesFrom.foreach(f => {
      if(namesTo.contains(f.getName)){
        val f2 = namesTo(f.getName)
        val targetFile = new File(diffs.getAbsolutePath + s"/${f.getName}_diff.txt")
        val toExecute = s"diff ${f.getAbsolutePath} ${f2.getAbsolutePath}"
        val targetFilePath = targetFile.getAbsolutePath
        (toExecute #> new File(targetFilePath)).!
      } else{
        println(s"$f got deleted")
      }
    })
  }

  def calculateDiff(from: File, to: File) = {
    logger.trace("calculating diff from {} to {}",from.getAbsolutePath,to.getAbsolutePath)
    extractAll(from, FROM)
    extractAll(to, TO)
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

  def calculateAllDiffs() = {
    val files = new File(directory)
      .listFiles()
      .toSeq
      .filter(_.getName.endsWith(".zip"))
      .sorted
    files.slice(0,files.size-1).zip(files.slice(1,files.size))
      .foreach{case (from,to) => calculateDiff(from,to)}
  }

}
