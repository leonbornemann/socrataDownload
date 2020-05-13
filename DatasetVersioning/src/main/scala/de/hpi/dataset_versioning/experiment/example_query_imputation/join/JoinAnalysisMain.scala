package de.hpi.dataset_versioning.experiment.example_query_imputation.join

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.experiment.example_query_imputation.QueryRelationshipDiscoverer
import de.hpi.dataset_versioning.experiment.example_query_imputation.projection.ProjectionAnalysisMain.{args, logger}
import de.hpi.dataset_versioning.experiment.example_query_imputation.projection.ProjectionInfo
import de.hpi.dataset_versioning.io.IOService

import scala.io.Source

object JoinAnalysisMain extends App with StrictLogging{
  /*IOService.socrataDir = args(0)
  val startVersion = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val endVersion = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  IOService.cacheMetadata(version)
  val a = IOService.cachedMetadata(version)
  val f = IOService.getInferredJoinFile(startVersion,endVersion)
  val it = Source.fromFile(f).getLines()
  it.next()
  val joinSet = it.toSeq
    .map(s => {
      val tokens = s.split(",")
      JoinInfo(tokens(0),tokens(1),tokens(2))
    })
  joinSet.foreach(jI => {
    logger.debug(s"Detected Join: $jI")
    val joined = IOService.tryLoadDataset(jI.joinedDSID,version)
    val pk = IOService.tryLoadDataset(jI.pkDSId,version)
    val fk = IOService.tryLoadDataset(jI.fkDSID,version)
    logger.debug(a(pk.id).resource.name + s"(${a(pk.id).link})")
    pk.print()
    logger.debug("with")
    logger.debug(a(fk.id).resource.name + s"(${a(fk.id).link})")
    fk.print()
    logger.debug("to")
    logger.debug(a(joined.id).resource.name + s"(${a(joined.id).link})")
    joined.print()
    println()
    println()
    println()
    println()
    println()
  })*/

}
