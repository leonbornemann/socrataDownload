package de.hpi.dataset_versioning.crawl.data.parser

import java.io.File

object JsonParsingMain extends App {
  val dirWithUncompressedFiles = args(0)
  new JsonDataParser().parseAllJson(new File(dirWithUncompressedFiles))
  //TODO: ignore the relational approach, make this entity, property, value?
}
