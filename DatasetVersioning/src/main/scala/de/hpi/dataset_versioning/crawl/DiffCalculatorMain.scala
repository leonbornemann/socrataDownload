package de.hpi.dataset_versioning.crawl

import java.io.File

object DiffCalculatorMain extends App {

  val directory = args(0)
  val diffDirectory = args(1)
  val diffcalculator = new DiffCalculator(new File(diffDirectory))
  diffcalculator.calculateAllDiffs(directory)
  diffcalculator.deleteUnmeaningfulDiffs()
}
