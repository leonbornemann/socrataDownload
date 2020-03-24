package de.hpi.dataset_versioning.data.metadata.custom.joinability

import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.io.IOService

object ManualJoinabilityEdgeTestCreatorMain extends App {
  IOService.socrataDir = args(0)
  val explorer = new JoinabilityGraphExplorer
  explorer.createManualLookupTests(LocalDate.parse(args(1),IOService.dateTimeFormatter),
    new File(args(2)),
    new File(args(3)),
    new File(args(4)),
    new File(args(5)))

}
