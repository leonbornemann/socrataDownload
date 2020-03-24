package de.hpi.dataset_versioning.data.metadata.custom.joinability

import java.io.File

object JoinabilityGraphSizeReductionMain extends App {
  val explorer = new JoinabilityGraphExplorer()
  explorer.transformToSmallRepresentation(new File(args(0)),new File(args(1)),new File(args(2)),new File(args(3)))
  //explorer.explore(path)

}
