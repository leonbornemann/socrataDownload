package de.hpi.dataset_versioning.experiment.example_query_imputation


object JoinVariant extends Enumeration {
  type JoinVariant = Value
  val PK_TO_PK,FK_TO_PK = Value
}
