package de.hpi.dataset_versioning.crawl

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.crawl.SocrataCrawlMain.args

object DeepScrollingTest extends App {
  new SocrataMetadataCrawler(null).deepScrollingTest()

}
