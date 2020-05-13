package de.hpi.dataset_versioning.data.exploration

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.exploration.RandomDiffToHTMLExport.args
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.matching.DatasetInstance

import scala.io.Source
import scala.util.Random

object DiffPairExport extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val sourceFile = args(1)
  val targetDir = args(2)
  val perPairCount = 10
  val exporter = new DatasetHTMLExporter()
  exportDiffPairs

  private def exportDiffPairs = {
    val lineages = IOService.readCleanedDatasetLineages
      .map(l => (l.id,l))
      .toMap
    val datasetPairs = Source.fromFile(sourceFile)
      .getLines()
      .toSeq
      .tail
      .map(l => {
        val tokens = l.split(",")
        //A,B,P(A AND B),P(B),P(A|B)
        ChangeCorrelationInfo(tokens(0),tokens(1),tokens(2).toInt,tokens(3).toInt,tokens(4).toDouble)
      })
    datasetPairs.foreach(dp => {
      val lA = lineages(dp.idA)
      val lB = lineages(dp.idB)
      logger.debug(s"Processing ${lA.id} and ${lB.id}")
      val sharedChangeTimestamps = Random.shuffle(lA.versionsWithChanges.tail.intersect(lB.versionsWithChanges.tail))
      var i=0
      while(i< perPairCount && i<sharedChangeTimestamps.size){
        val curDiffTimestamp = sharedChangeTimestamps(i)
        val prevVersionA = lA.versionsWithChanges.takeWhile(_.isBefore(curDiffTimestamp)).last
        val prevVersionB = lB.versionsWithChanges.takeWhile(_.isBefore(curDiffTimestamp)).last
        val dsABeforeChange = IOService.tryLoadDataset(DatasetInstance(lA.id, prevVersionA), true)
        val dsAAfterChange = IOService.tryLoadDataset(DatasetInstance(lA.id, curDiffTimestamp), true)
        val diffA = dsABeforeChange.calculateDataDiff(dsAAfterChange)
        val dsBBeforeChange = IOService.tryLoadDataset(DatasetInstance(lB.id, prevVersionB), true)
        val dsBAfterChange = IOService.tryLoadDataset(DatasetInstance(lB.id, curDiffTimestamp), true)
        val diffB = dsBBeforeChange.calculateDataDiff(dsBAfterChange)
        if(Seq(dsAAfterChange,dsABeforeChange,dsBAfterChange,dsBBeforeChange).exists(_.erroneous)){
          logger.debug(s"Skipping version $curDiffTimestamp, because one dataset was erroneous")
          logger.debug(s"Error while parsing existed?: ${Seq(dsAAfterChange,dsABeforeChange,dsBAfterChange,dsBBeforeChange).map(d => {(d.id,d.version,d.erroneous)})}")
        } else{
          exporter.exportDiffPairToTableView(dsABeforeChange, dsAAfterChange, diffA,
            dsBBeforeChange, dsBAfterChange, diffB,
            new File(s"$targetDir/${lA.id}_AND_${lB.id}_$curDiffTimestamp.html"))
        }
        i+=1
      }
    })
  }
}

case class ChangeCorrelationInfo(idA: String, idB: String, P_A_AND_B: Int, P_B: Int, P_A_IF_B: Double)
