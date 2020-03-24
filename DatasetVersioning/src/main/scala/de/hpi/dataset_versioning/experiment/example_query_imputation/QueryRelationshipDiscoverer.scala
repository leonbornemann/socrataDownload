package de.hpi.dataset_versioning.experiment.example_query_imputation

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.custom.IdentifierMapping
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata
import de.hpi.dataset_versioning.data.metadata.custom.joinability.{JoinabilityGraph, JoinabilityGraphExplorer}
import de.hpi.dataset_versioning.data.metadata.custom.{CustomMetadata, IdentifierMapping}
import de.hpi.dataset_versioning.data.LoadedRelationalDataset
import de.hpi.dataset_versioning.experiment.example_query_imputation.join.JoinVariant
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

class QueryRelationshipDiscoverer(version:LocalDate) extends StrictLogging{

  def logProgress(thingToProcess:String,count: Int, modulo: Int, size: Option[Int] = None) = {
    if(count % modulo==0 && size.isDefined)
      logger.debug(s"processed $count out of ${size.get} $thingToProcess (${100*count/size.get.toFloat}%)")
    else if (count % modulo==0){
      logger.debug(s"processed $count $thingToProcess")
    }
  }

  def findJoins(version: LocalDate) = {
    val stringIdToMetadata = IOService.cachedMetadata(version)
    IOService.cacheCustomMetadata(version)
    val customMetadata = IOService.cachedCustomMetadata(version)
    val hashToDs = customMetadata.values.groupBy(_.tupleSpecificHash)
    val checkedJoins = mutable.HashSet[(Set[Int],(Short,Short))]()
    val resultWriter = new PrintWriter(IOService.getInferredJoinFile(version))
    resultWriter.println("joinedDataset,primaryKeyJoinPart,foreignKeyJoinPart")
    /*the following code uses these abbreviations:
    j - the candidate dataset for the join result
    fk - the candidate dataset for the relation containing the foreign key
    pk - the candidate dataset for the relation containing the primary key
    DS - Dataset
    Col - Column
     */
    var processedDsCount = 0
    var processedEdges = 0
    var executedJoinQueries = 0
    var discoveredJoins = 0
    for(jDS <- graph.adjacencyListGroupedByDS.keySet) {
      for((fkDS,edges) <- graph.adjacencyListGroupedByDS(jDS)) {
        for(((jCol,fkCol),jInFKContainment) <- edges){
          val fkInJContainment = graph.getEdgeValue(fkDS,jDS,fkCol,jCol)
          val jDSStringID = numericIdToDS(jDS)
          val fkDSStringID = numericIdToDS(fkDS)
          if(jInFKContainment == 1.0 && fkInJContainment == 1.0 && customMetadata(jDSStringID).nrows == customMetadata(fkDSStringID).nrows){ //todo: REMOVE NROW FILTER if selection is allowed
            //find primary key candidates:
            for(((pkDS,pkCol),fkInPkContainment) <- graph.adjacencyListGroupedByDSAndCol((fkDS,fkCol))){
              val curJoinCandidate = (Set(fkDS, pkDS), (pkCol, fkCol))
              val pkDSStringID = numericIdToDS(pkDS)
              val ncolJ = customMetadata(jDSStringID).ncols
              val ncolPk = customMetadata(pkDSStringID).ncols
              val ncolFk = customMetadata(fkDSStringID).ncols
              if(fkInPkContainment==1.0 && !checkedJoins.contains(curJoinCandidate) && (ncolJ == ncolPk + ncolFk || ncolJ == ncolPk + ncolFk -1)){ //todo: REMOVE schema FILTER IF PROJECTION IS ALLOWED
                checkedJoins.add(curJoinCandidate)
                val pkDsLoaded = IOService.tryLoadAndCacheDataset(numericIdToDS(pkDS),version)
                val fkDsLoaded = IOService.tryLoadAndCacheDataset(numericIdToDS(fkDS),version)
                val joinedDSKeepBoth = pkDsLoaded.join(fkDsLoaded,numericIdToCol((pkDS,pkCol)),numericIdToCol((fkDS,fkCol)),JoinVariant.KeepBoth)
                val joinedDSKeepLeft = pkDsLoaded.join(fkDsLoaded,numericIdToCol((pkDS,pkCol)),numericIdToCol((fkDS,fkCol)),JoinVariant.KeepLeft)
                val joinedDSKeepRight = pkDsLoaded.join(fkDsLoaded,numericIdToCol((pkDS,pkCol)),numericIdToCol((fkDS,fkCol)),JoinVariant.KeepRight)
                discoveredJoins += findAndCheckJoinMatches(version, hashToDs, resultWriter, fkDSStringID, pkDSStringID, joinedDSKeepBoth)
                discoveredJoins += findAndCheckJoinMatches(version, hashToDs, resultWriter, fkDSStringID, pkDSStringID, joinedDSKeepLeft)
                discoveredJoins += findAndCheckJoinMatches(version, hashToDs, resultWriter, fkDSStringID, pkDSStringID, joinedDSKeepRight)
                executedJoinQueries +=1
                logProgress("Join Queries",executedJoinQueries,1000)
              }
            }
          }
          processedEdges +=1
          logProgress("Joinability Graph Edges",processedEdges,10000)
          if(processedEdges%10000==0){
            logger.debug(s"Found $discoveredJoins true join relationships")
          }
        }
      }
      processedDsCount +=1
      logProgress("datasets",processedDsCount,1000,Some(graph.adjacencyListGroupedByDS.size))
    }
    resultWriter.close()
  }


  private def findAndCheckJoinMatches(version: LocalDate, hashToDs: Map[Int, Iterable[CustomMetadata]], resultWriter: PrintWriter, fkDSStringID: String, pkDSStringID: String, joinedDSKeepBoth: LoadedRelationalDataset) = {
    val joinedHash = joinedDSKeepBoth.getTupleSpecificHash
    //find all candidates that have matching tuple specific hash values:
    var foundMatches = 0
    for (matchCandidate <- hashToDs.getOrElse(joinedHash, Seq())) {
      //validate Candidate:
      val candidateDs = IOService.tryLoadAndCacheDataset(matchCandidate.id, version)
      val mapping = getColumnMapping(candidateDs, joinedDSKeepBoth, None)
      if (mapping.isDefined && candidateDs.isEqualTo(joinedDSKeepBoth, mapping.get)) {
        resultWriter.println(s"${matchCandidate.id},$pkDSStringID,$fkDSStringID")
        resultWriter.flush()
        foundMatches+=1
      }
    }
    foundMatches
  }

  val explorer = new JoinabilityGraphExplorer
  val numericIdToDS = IdentifierMapping.readDatasetIDMapping_intToString(IOService.getDatasetIdMappingFile(version))
  val numericIdToCol = IdentifierMapping.readColumnIDMapping_shortToString(IOService.getColumnIdMappingFile(version))
  val graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(version))
  logger.trace("Finished Projection Finder Initialization")

  /***
   * Assumes unchanged column names - which provides the schema mapping!
   * @param originalDS
   * @param projectedDS
   * @return
   */
  def isProjection(originalDS: LoadedRelationalDataset, projectedDS: LoadedRelationalDataset,projectedToOriginalMapping:mutable.HashMap[String,String]): Boolean = {
    //some early aborts:
    if(originalDS.ncols < projectedDS.ncols)
      false
    else {
      if(projectedDS.colNames.toSet!=projectedToOriginalMapping.keySet){
        println()
      }
      val columnRenames = projectedDS.colNames.map(s => (projectedToOriginalMapping(s),s))
      val projectedFromOriginal = originalDS.getProjection(originalDS.id + "_projected",columnRenames)
      assert(projectedFromOriginal.colNames == projectedDS.colNames)
      projectedFromOriginal.tupleSetEqual(projectedDS)
    }
  }

  def getColumnMapping(from: LoadedRelationalDataset, to: LoadedRelationalDataset, edges: Option[Set[(String, String)]]) = {
    var abort = false
    val columnMapping = mutable.HashMap[String,String]()
    val takenColumns = scala.collection.mutable.HashSet[String]()
    if(from.columnHashes.isEmpty) from.calculateColumnHashes()
    if(to.columnHashes.isEmpty) to.calculateColumnHashes()
    //if we have edges from the joinability graph we can use those:
    if(edges.isDefined){
      val map = edges.get.groupBy(_._1)
        .toSeq
        .sortBy(_._2.size)
      val a = map.iterator
      while(a.hasNext && !abort){
        var (srcCol,edges) = a.next()
        val candidateMatches = edges.map(_._2)
          .filter(!takenColumns.contains(_))
        if(candidateMatches.size==0)
          abort = true
        else if(edges.size==1){
          takenColumns += candidateMatches.head
          columnMapping(srcCol) = candidateMatches.head
        } else{
          takenColumns += candidateMatches.head
          columnMapping(srcCol) = candidateMatches.head
          logger.debug("encountered difficult column matching case")
        }
      }
    }
    if(abort){
      None
    } else{
      //try to match the rest of the columns:
      val freeInProjected = from.colNames.toSet.diff(columnMapping.keySet)
        .map(s => (s,from.columnHashes(s)))
        .toMap
      if(freeInProjected.isEmpty)
        Some(columnMapping)
      else{
        val freeInOriginal = to.colNames.toSet.diff(columnMapping.values.toSet) //TODO: make the map the other way around here! - saves code later
          .map(s => (s,to.columnHashes(s)))
          .toMap
        //invert the maps:
        val projectedHashToColname = freeInProjected.groupBy(_._2).mapValues(_.keys)
        val originalHashToColname = freeInOriginal.groupBy(_._2).mapValues(_.keys)
        val it = projectedHashToColname.iterator
        while(it.hasNext && !abort){
          val (projectedHash,colNamesProjected) = it.next()
          if(!originalHashToColname.contains(projectedHash)){
            abort = true
          } else {
            val matchingColNames = originalHashToColname(projectedHash).toSeq
              .filter(!takenColumns.contains(_))
            if (colNamesProjected.size == matchingColNames.size) {
              columnMapping ++= colNamesProjected.zip(matchingColNames)
              takenColumns ++= matchingColNames
            } else {
              abort = true
            }
          }
        }
        if(abort) None
        else Some(columnMapping)
      }
    }
  }

  def findProjections(version: LocalDate) = {
    IOService.cacheMetadata(version)
    val stringIdToMetadata = IOService.cachedMetadata(version)
    //TODO: testing the graph for consistency
    val dsToProjectionCandidates = graph.adjacencyListGroupedByDS.keySet.map(dsID => {
      val strID = numericIdToDS(dsID)
      val ncols = stringIdToMetadata(strID).resource.columns_name.size
      val maptoOtherdatasets = graph.adjacencyListGroupedByDS(dsID)
      val sortedProjectionCandidates = maptoOtherdatasets.toIndexedSeq.map{case (id,edges) => {
        val newEdges = edges.filter(_._2 == 1.0) //values in the column of the projection need to be 100% contained in the other column
          .map(e => e._1) //we are only interested in how many outgoing edges of distinct columns we have
          .toSet
        (id,newEdges,newEdges.map(_._1).toSet.size)
        }}.filter(_._3>0)
        .sortBy{-_._3}
      val highestLength = if(sortedProjectionCandidates.isEmpty) -1 else sortedProjectionCandidates.head._3
      val bestCandidates = sortedProjectionCandidates.takeWhile(t => t._3==highestLength) //Only highest length can be candidates
      //logger.debug(s"For $strID found ${bestCandidates.size} projection candidates with $highestLength columns contained (ncols of this dataset according to metadata: $ncols)")
      (dsID,ncols,highestLength,bestCandidates)
    }).filter(!_._4.isEmpty)
      .toIndexedSeq
    logger.trace("Finished generating candidate lists")
    val sortedProjectionCandidates = dsToProjectionCandidates
      .filter{case (_,_,numColsOverlap,bestCandidates) => numColsOverlap>0 && !bestCandidates.isEmpty}
      .sortBy{case (_,ncol,numColsOverlap,_) => - numColsOverlap / Math.max(numColsOverlap,ncol).toFloat}
    /*sortedProjectionCandidates.take(100)
      .foreach{ case (dsID,ncols,ncolsOverlap,bestCandidates) => {
        val strID = numericIdToDS(dsID)
        logger.debug(s"Found ${bestCandidates.size} Projection Candidates for $strID (overlap detected in $ncolsOverlap out of $ncols):")
        bestCandidates.foreach(c => {
          val strIDCandidate = numericIdToDS(c._1)
          logger.debug(s"\t$strIDCandidate")
        })
      }}*/
    logger.debug(s"Going through ${sortedProjectionCandidates.size} candidate lists")
    var checkCount = 0
    var numHitsDifferentColnames = 0
    var numHitsSameColnames = 0
    val pr = new PrintWriter(IOService.getInferredProjectionFile(version))
    pr.println("originalDataset,projectedDataset,originalNcols,projectedNcols")
    for(i <- 0 until sortedProjectionCandidates.size) {
      val (projectedID,_,_,candidates) = sortedProjectionCandidates(i)
      val projectedDSStringID = numericIdToDS(projectedID)
      val projectedDS = IOService.tryLoadDataset(projectedDSStringID,version)
      IOService.cacheDataset(projectedDS)
      projectedDS.calculateColumnHashes()
      if(!projectedDS.isEmpty && projectedDS.ncols>1){
        for((srcID,edges,_) <- candidates) {
          val srcDSStringID = numericIdToDS(srcID)
          val originalDS = IOService.tryLoadDataset(srcDSStringID,version)
          IOService.cacheDataset(originalDS)
          originalDS.calculateColumnHashes()
          val columnMapping = getColumnMapping(projectedDS,originalDS,Some(edges.map{case (c1,c2) => (numericIdToCol((projectedID,c1)),numericIdToCol((srcID,c2)))}))
          if(columnMapping.isDefined && isProjection(originalDS,projectedDS,columnMapping.get)){
            pr.println(s"${originalDS.id},${projectedDS.id},${originalDS.ncols},${projectedDS.ncols}")
            pr.flush()
            if(columnMapping.get.keySet == columnMapping.get.values.toSet)
              numHitsSameColnames +=1
            else
              numHitsDifferentColnames +=1
          }
          checkCount +=1
          if(checkCount % 100 ==0){
            val numHits = numHitsDifferentColnames + numHitsSameColnames
            logger.debug(s"Found $numHits true projection in $checkCount tries (accuracy ${100*numHits / checkCount.toFloat}%)")
          }
        }
      }
      if(i %100 ==0) logger.debug(s"Processed ${i+1} out of ${sortedProjectionCandidates.size} lists (${100*(i+1)/sortedProjectionCandidates.size.toFloat}%)")
    }
    pr.close()
  }

}
