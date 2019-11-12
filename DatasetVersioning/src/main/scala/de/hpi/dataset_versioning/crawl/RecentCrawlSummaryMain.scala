package de.hpi.dataset_versioning.crawl

import java.io.{File, FileInputStream, PrintWriter, StringWriter}
import java.util.zip.{ZipEntry, ZipInputStream}

import com.google.gson.{JsonElement, JsonObject, JsonPrimitive}

import scala.collection.mutable

object RecentCrawlSummaryMain extends App{

  def readFileSizes(source1: String) = {
    val zis: ZipInputStream = new ZipInputStream(new FileInputStream(source1))
    var ze: ZipEntry = zis.getNextEntry();
    val map = mutable.HashMap[String,Long]()
    while(ze !=null){
      if(!ze.isDirectory){
        val nameTokens = ze.getName.split("/")
        val name = nameTokens(nameTokens.size-1)
        val size = ze.getSize
        map(name) = size
      }
      ze = zis.getNextEntry
    }
    zis.close()
    map
  }

  def zipVariant() = {
    val directory = args(0)
    val summaryFile = args(1)
    val files = new File(directory)
      .listFiles()
      .toSeq
      .filter(_.getName.endsWith(".zip"))
      .sorted
    println(files)
    val previous = files(files.size - 2)
    val source1 = previous.getAbsolutePath
    val current = files(files.size - 1)
    val source2 = current.getAbsolutePath
    val f1Map = readFileSizes(source1)
    val f2Map = readFileSizes(source2)
    val numNewFiles = f2Map.keySet.diff(f1Map.keySet).size
    val numDeletedFiles = f1Map.keySet.diff(f2Map.keySet).size
    val potentiallyChangedFiles = f2Map.keySet.intersect(f1Map.keySet)
    val numPotentiallyChangedFiles = potentiallyChangedFiles.size
    val pr = new StringWriter()//(summaryFile)
    pr.append(s"Displaying changes from ${previous.getName} to ${current.getName}\n")
    pr.append("#NewFiles: " + numNewFiles + "\n")
    pr.append("#DeletedFiles: " + numDeletedFiles + "\n")
    pr.append("#Files present in both days: " + numPotentiallyChangedFiles + "\n")
    pr.append("Size Last Day (raw json): " + (f1Map.values.sum / 1000000000.0)+ "GB\n")
    pr.append("Size This Day (raw json): " + (f2Map.values.sum / 1000000000.0)+ "GB\n")
    val filesWithChanges = potentiallyChangedFiles.map(f => {
          val version1 = f1Map(f)
          val version2 = f2Map(f)
          if(version1 != version2)
            Option((f,(version1-version2).abs))
          else
            None
        })
      .filter(_.isDefined)
      .map(_.get)
    pr.append("Actually Changed Files: " + filesWithChanges.size + "\n")
    pr.append("Top five changes: \n")
    filesWithChanges.toIndexedSeq.sortBy(t => -t._2)
      .take(5)
      .foreach(a =>pr.append(a.toString + "\n"))
    val max = filesWithChanges.map(_._2).max
    val min = filesWithChanges.map(_._2).min
    val step = (max - min) / 10
    println(max)
    println(min)
    println(step)
    //put into json to send via slack:
    if(max>0) {
      val buckets = (min + step).to(max - step).by(step)
        .zipWithIndex
      val bIDs = filesWithChanges.toSeq.map { case (f, changes) => {
        val bidO = buckets.find(b => (changes < b._1))
        val bID = if(bidO.isDefined) bidO.get._2 else buckets.size
        bID
      }
      }
      val histogram = bIDs.groupBy(identity)
        .mapValues(_.size)
      pr.append("Change Size distribution:")
      pr.append("Bucket,Border [#BytesChanged<],Count\n")
      0.until(buckets.size).foreach(i => {
        val border = buckets(i)._1
        val count = histogram.getOrElse(i, 0)
        pr.append(s"$i,$border,$count\n")
      })
      pr.append(s"${buckets.size},>${buckets.last._1},${histogram.getOrElse(buckets.size, 0)}\n")
    }
    val json = new JsonObject()
    json.add("text",new JsonPrimitive(pr.toString))
    val writer = new PrintWriter(summaryFile)
    writer.println(json.toString)
    writer.close()
  }

  zipVariant()

}
