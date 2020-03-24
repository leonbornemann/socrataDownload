package de.hpi.dataset_versioning.experiment.example_query_imputation.join

object JoinVariant extends Enumeration {
  type JoinVariant = Value
  val KeepLeft,KeepRight,KeepBoth = Value
}
