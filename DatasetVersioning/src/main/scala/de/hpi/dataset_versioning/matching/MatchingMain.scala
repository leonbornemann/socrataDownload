package de.hpi.dataset_versioning.matching

import de.hpi.dataset_versioning.io.IOService

object MatchingMain extends App {

  IOService.socrataDir = args(0)
  val matchingRunner = new MatchingRunner()
  matchingRunner.evaluateMatchings
}
