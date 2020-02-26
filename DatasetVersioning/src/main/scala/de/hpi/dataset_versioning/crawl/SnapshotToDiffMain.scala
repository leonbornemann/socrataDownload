package de.hpi.dataset_versioning.crawl

import java.io.File

object SnapshotToDiffMain extends App {
  val transformer = new SnapshotToDiffTransformer(new File(args(0)),new File(args(1)),7)
  transformer.transformAll()
}
