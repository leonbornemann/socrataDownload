package de.hpi.dataset_versioning.experiment.example_query_imputation.join

object JoinConstructionVariant extends Enumeration {
  type JoinConstructionVariant = Value
  val KeepLeft,KeepRight,KeepBoth = Value
}
