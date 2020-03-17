package de.hpi.dataset_versioning.experiment

import java.io.File

import scala.io.Source

object JoinabilityGraphSizeReductionMain extends App {
  val explorer = new JoinabilityGraphExplorer()
  explorer.transformToSmallRepresentation(new File(args(0)),new File(args(1)),new File(args(2)),new File(args(3)))
  //explorer.explore(path)

}
