package de.hpi.dataset_versioning.data.exploration.db_synthesis

import java.io.PrintWriter
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.exploration.db_synthesis.ColnameHistogram.args
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object ConditionalProbabilityExplorationMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val pr = new PrintWriter(args(1))
  val lineageSizeFilter = args(2).toInt
  var histories = DatasetVersionHistory.fromJsonObjectPerLineFile(IOService.getCleanedVersionHistoryFile().getAbsolutePath)
  histories = histories.filter(_.versionsWithChanges.size > lineageSizeFilter)
  val idToVersions = histories.map(h => (h.id,h.versionsWithChanges.filter(_ != IOService.STANDARD_TIME_FRAME_START).toSet))
    .toMap
  val keys = idToVersions.keySet.toIndexedSeq.sorted
  pr.println("A,B,P(A AND B),P(B),P(A|B)")
  for(i <- 0 until keys.size){
    for(j <- (i+1) until keys.size){
      val curKeyA = keys(i)
      val curKeyB = keys(j)
      //first variant:
      val bOccurrences = idToVersions(curKeyB)
      val aOccurrences = idToVersions(curKeyA)
      val p_B = bOccurrences.size
      val p_A = aOccurrences.size
      val p_A_AND_B = bOccurrences.intersect(aOccurrences).size
      val probAIfB = p_A_AND_B / p_B.toDouble
      val probBIfA = p_A_AND_B / p_A.toDouble
      pr.println(s"$curKeyA,$curKeyB,$p_A_AND_B,$p_B,$probAIfB")
      pr.println(s"$curKeyB,$curKeyA,$p_A_AND_B,$p_A,$probBIfA")
    }
    logger.trace(s"Finished ${i+1} out of ${keys.size} iterations (${i/keys.size.toFloat}%)")
  }
  /*val hashMap = mutable.HashMap[LocalDate,mutable.HashSet[String]]()
  histories.foreach(h => {
    h.versionsWithChanges.foreach(v => {
      val dayMap = hashMap.getOrElseUpdate(v,mutable.HashSet[String]())
      dayMap.add(h.id)
    })
  })
  var curDay = IOService.STANDARD_TIME_FRAME_START
  val finalDay = IOService.STANDARD_TIME_FRAME_END
  while(!curDay.isAfter(finalDay)){
    val changeSetThisDay =hashMap(curDay)
    curDay = curDay.plusDays(1)
  }*/
}
