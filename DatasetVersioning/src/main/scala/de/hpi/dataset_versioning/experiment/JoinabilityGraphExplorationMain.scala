package de.hpi.dataset_versioning.experiment

import java.io.File


object JoinabilityGraphExplorationMain extends App {
  val path = args(0)
  val explorer = new JoinabilityGraphExplorer()
  explorer.explore(path)
}
