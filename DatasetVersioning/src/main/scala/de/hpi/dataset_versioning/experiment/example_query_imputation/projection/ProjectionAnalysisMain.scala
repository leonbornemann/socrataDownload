package de.hpi.dataset_versioning.experiment.example_query_imputation.projection

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

object ProjectionAnalysisMain extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val version = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val f = IOService.getInferredProjectionFile(version)
  val it = Source.fromFile(f).getLines()
  it.next()
  val projectionSet = it.toSeq
    .map(s => {
      val tokens = s.split(",")
      ProjectionInfo(tokens(0),tokens(1),tokens(2).toInt,tokens(3).toInt)
    })
    .filter(_.ncolsProjection>1)
  val actualProjections = projectionSet.filter(info => info.ncolsOriginal != info.ncolsProjection)
  println(projectionSet.size)
  println(actualProjections.size)
  /*actualProjections.foreach(pI => {
    logger.debug(s"Detected Projection: $pI")
    val ds1 = IOService.tryLoadDataset(pI.originalID,version)
    val ds2 = IOService.tryLoadDataset(pI.projectedID,version)
    ds1.print()
    logger.debug("to")
    ds2.print()
    logger.debug("")
  })*/
  val datasets = actualProjections.flatMap(pI => Set(pI.originalID)).toSet
  val datasetsProjected = actualProjections.flatMap(pI => Set(pI.projectedID)).toSet
  logger.debug(s"${datasets.size}")
  logger.debug(s"${datasetsProjected.size}")

}
