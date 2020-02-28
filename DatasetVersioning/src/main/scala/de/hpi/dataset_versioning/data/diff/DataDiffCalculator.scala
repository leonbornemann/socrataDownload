package de.hpi.dataset_versioning.data.diff

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.matching.MatchingRunner

class DataDiffCalculator() extends StrictLogging{

  def calculateDataDiff(previous: LocalDate, current: LocalDate) = {
    val matching = new MatchingRunner().getGroundTruthMatching(previous,current)
    var numInserts = 0
    var numDeletes = 0
    var numTupleChanges = 0
    var numProjections = 0
    var numColumnInserts = 0
    var processed = 0
    var numDataChanges = 0
    var numDiffIncomplete = 0
    matching.matchings.foreach{ case (prev,cur) => {
      try {
        val previousLoadedDataset = IOService.loadDataset(prev)
        val currentLoadedDataset = IOService.loadDataset(cur)
        val dataDiff = previousLoadedDataset.calculateDataDiff(currentLoadedDataset)
        if(!dataDiff.incomplete) {
          if (!dataDiff.inserts.isEmpty) numInserts += 1
          if (!dataDiff.deletes.isEmpty) numDeletes += 1
          if (!dataDiff.updates.isEmpty) numTupleChanges += 1
        }
        if(dataDiff.schemaChange.projection.isDefined) numProjections +=1
        if(dataDiff.schemaChange.columnInsert.isDefined) numColumnInserts +=1
        if(dataDiff.incomplete) numDiffIncomplete +=1
        if(!dataDiff.inserts.isEmpty || !dataDiff.deletes.isEmpty || !dataDiff.updates.isEmpty) numDataChanges +=1
//        if (!dataDiff.inserts.isEmpty || !dataDiff.deletes.isEmpty || dataDiff.updates.isEmpty) {
//          logger.debug(s"Calculated Diff between $previous and $current:")
//          logger.debug(s"original Table $previous:")
//          previousLoadedDataset.print()
//          logger.debug(s"original Table $current:")
//          currentLoadedDataset.print()
//          logger.debug(s"Data Diff:")
//          dataDiff.print()
//          println()
//        }
      } catch {
        case _:Throwable => logger.debug(s"Exception while trying to parse either $prev or $cur")
      }
      processed +=1
      if(processed%1000==0){
        printDiffSummary(numInserts, numDeletes, numTupleChanges, numProjections, processed, numDataChanges, numDiffIncomplete)
      }
    }}
    logger.debug(s"Done")
    printDiffSummary(numInserts, numDeletes, numTupleChanges, numProjections, processed, numDataChanges, numDiffIncomplete)
  }

  private def printDiffSummary(numInserts: Int, numDeletes: Int, numTupleChanges: Int, numProjections: Int, processed: Int, numDataChanges: Int, numDiffIncomplete: Int) = {
    logger.trace(s"------------------------------------------------")
    logger.trace(s"processed: $processed")
    logger.trace(s"numInserts: $numInserts")
    logger.trace(s"numDeletes: $numDeletes")
    logger.trace(s"numTupleChanges: $numTupleChanges")
    logger.trace(s"projections: $numProjections")
    logger.trace(s"Column Inserts: $numInserts")
    logger.trace(s"num Incomplete Diffs: $numDiffIncomplete")
    logger.trace(s"$numDataChanges")
    logger.trace(s"------------------------------------------------")
  }
}
