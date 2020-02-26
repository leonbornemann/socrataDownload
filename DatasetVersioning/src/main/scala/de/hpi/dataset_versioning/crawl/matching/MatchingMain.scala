package de.hpi.dataset_versioning.crawl.matching

object MatchingMain extends App {

  IOService.socrataDir = args(0)
  IOService.workingDir = args(1)
  val matchingRunner = new MatchingRunner()
  matchingRunner.evaluateMatchings
}
