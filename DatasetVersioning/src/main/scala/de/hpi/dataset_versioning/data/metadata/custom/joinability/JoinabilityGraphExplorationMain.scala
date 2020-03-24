package de.hpi.dataset_versioning.data.metadata.custom.joinability

object JoinabilityGraphExplorationMain extends App {
  val path = args(0)
  val explorer = new JoinabilityGraphExplorer()
  explorer.explore(path)
}
