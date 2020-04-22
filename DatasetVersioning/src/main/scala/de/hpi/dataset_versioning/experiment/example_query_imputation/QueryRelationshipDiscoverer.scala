package de.hpi.dataset_versioning.experiment.example_query_imputation

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.custom.{ColumnDatatype, CustomMetadata, IdentifierMapping}
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata
import de.hpi.dataset_versioning.data.metadata.custom.joinability.{ColEdge, DatasetColumnVertex, JoinabilityGraph, JoinabilityGraphExplorer}
import de.hpi.dataset_versioning.data.LoadedRelationalDataset
import de.hpi.dataset_versioning.experiment.example_query_imputation.JoinVariant.JoinVariant
import de.hpi.dataset_versioning.experiment.example_query_imputation.join.JoinConstructionVariant
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.util.TableFormatter

import scala.collection.mutable

class QueryRelationshipDiscoverer(version:LocalDate) extends StrictLogging{

  val explorer = new JoinabilityGraphExplorer
  val numericIdToDS = IdentifierMapping.readDatasetIDMapping_intToString(IOService.getDatasetIdMappingFile(version))
  val stringIdToDS = IdentifierMapping.readDatasetIDMapping_stringToInt(IOService.getDatasetIdMappingFile(version))
  val numericIdToCol = IdentifierMapping.readColumnIDMapping_shortToString(IOService.getColumnIdMappingFile(version))
  val stringColToNumeric = IdentifierMapping.readColumnIDMapping_stringToShort(IOService.getColumnIdMappingFile(version))
  var graph:JoinabilityGraph = null
  logger.trace("Finished Projection Finder Initialization")

  def logProgress(thingToProcess:String,count: Int, modulo: Int, size: Option[Int] = None) = {
    if(count % modulo==0 && size.isDefined)
      logger.debug(s"processed $count out of ${size.get} $thingToProcess (${100*(count/size.get.toFloat)}%)")
    else if (count % modulo==0){
      logger.debug(s"processed $count $thingToProcess")
    }
  }

  def graphContainsAllEdges(graph: JoinabilityGraph,jMetadata:CustomMetadata, jDS:Int, fkDS: Int, pkDS: Int): Boolean = {
    val it = jMetadata.columnMetadata.iterator
    var allContained = true
    while(it.hasNext){
      val (cname,cMetadata) = it.next()
      if(cMetadata.dataType!=ColumnDatatype.Numeric){
        val joinColID =stringColToNumeric((jDS,cname))._1
        val colEdges = graph.adjacencyListGroupedByDSAndCol.getOrElse(DatasetColumnVertex(jDS,joinColID), Map[DatasetColumnVertex, Float]())
        val adjacentCols = colEdges.iterator
        var containsOne = false
        while(adjacentCols.hasNext && !containsOne){
          val curNeighbour = adjacentCols.next()
          if(curNeighbour._1.dsID == fkDS || curNeighbour._1.dsID==pkDS)
            containsOne = true
        }
        if(!containsOne)
          allContained = false
      }
    }
    allContained
  }

  def addToFilterAnalysis(filterAnalysisMap: mutable.HashMap[String, FilterAnalysis], str: String, bool: Boolean) = {
    if(!bool) filterAnalysisMap(str).numFiltered +=1
    filterAnalysisMap(str).numCalled +=1
  }

  def printFilterPerformance(filterAnalysisMap: mutable.HashMap[String, FilterAnalysis]) = {
    val header = Seq("filterName,worksFor,numFiltered:Int,numCalled,selectivity".split(",").toSeq)
    val content = filterAnalysisMap.values.toSeq.map(v => Seq(v.filterName,v.worksFor,v.numFiltered,v.numCalled,v.selectivity))
    val table = header++content
    val toPrint = TableFormatter.format(table)
    logger.debug("Filter Performance:\n" + toPrint)
  }

  def findJoins(version: LocalDate, variant:JoinVariant=JoinVariant.FK_TO_PK) = {
    IOService.cacheCustomMetadata(version)
    val customMetadata = IOService.cachedCustomMetadata(version)
    logger.trace("Switching graph representation")
    if(variant == JoinVariant.PK_TO_PK)
      graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(version),1.0f)
    else
      graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(version))
    graph.switchToAdjacencyListGroupedByDSAndCol()
    val numEdges = graph.numEdges()
    logger.trace("Done switching graph representation")
    val hashToDs = customMetadata.values.groupBy(_.tupleSpecificHash)
    val checkedJoins = mutable.HashSet[(Int,Int,Short,Short)]()
    val resultWriter = new PrintWriter(IOService.getInferredJoinFile(version))
    resultWriter.println("joinedDataset,primaryKeyJoinPart,foreignKeyJoinPart")
    /*the following code uses these abbreviations:
    j - the candidate dataset for the join result
    fk - the candidate dataset for the relation containing the foreign key
    pk - the candidate dataset for the relation containing the primary key
    DS - Dataset
    Col - Column
     */
    var processedEdges = 0
    var executedJoinQueries = 0
    var discoveredJoins = 0
    val filterAnalysisMap = mutable.HashMap[String,FilterAnalysis]()
    filterAnalysisMap.put("jInFkContainment",FilterAnalysis("jInFkContainment","Join,Projection*",0,0))
    filterAnalysisMap.put("fkInJContainment",FilterAnalysis("fkInJContainment","Join,Projection*",0,0))
    filterAnalysisMap.put("J.nrow=FK.nrow",FilterAnalysis("J.nrow=FK.nrow","Join,Projection",0,0))
    filterAnalysisMap.put("PK.pkCol.uniqueness=1.0",FilterAnalysis("PK.pkCol.uniqueness=1.0","Join,Projection,Selection",0,0))
    filterAnalysisMap.put("fkInPkContainment",FilterAnalysis("fkInPkContainment","Join,Projection,Selection",0,0))
    filterAnalysisMap.put("checkedJoin",FilterAnalysis("checkedJoin","Join,Projection,Selection",0,0))
    filterAnalysisMap.put("J.ncol == Pk.ncol + Fk.ncol - 1",FilterAnalysis("J.ncol == Pk.ncol + Fk.ncol - 1","Join,Selection",0,0))
    filterAnalysisMap.put("J to FK and PK full edge containment",FilterAnalysis("J to FK and PK full edge containment","Join,Projection,Selection",0,0))
    for(DatasetColumnVertex(jDS,jCol) <- graph.adjacencyListGroupedByDSAndCol.keySet) {
      val jDSStringID = numericIdToDS(jDS)
      for ((DatasetColumnVertex(fkDS, fkCol), jInFKContainment) <- graph.adjacencyListGroupedByDSAndCol(DatasetColumnVertex(jDS, jCol))) {
        //val jInFKContainment = graph.getEdgeValue(jDS,fkDS,jCol,fkCol)
        val fkDSStringID = numericIdToDS(fkDS)
        val fkInJContainment = graph.getEdgeValue(fkDS, jDS, fkCol, jCol)
        if (jDS != fkDS && jInFKContainment == 1.0 && fkInJContainment == 1.0 && customMetadata(jDSStringID).nrows == customMetadata(fkDSStringID).nrows) { //todo: REMOVE NROW FILTER if selection is allowed
          //find primary key candidates:
          for ((DatasetColumnVertex(pkDS, pkCol), fkInPkContainment) <- graph.adjacencyListGroupedByDSAndCol(DatasetColumnVertex(fkDS, fkCol))) {
            val pkDSStringID = numericIdToDS(pkDS)
            val pkColStringID = numericIdToCol(pkDS,pkCol)
            if(jDS != pkDS && customMetadata(pkDSStringID).columnMetadata(pkColStringID).uniqueness == 1.0) { //todo: RELAX THIS IF WE WANT TO ACCEPT DATA ERRORS
              val curJoinCandidate = (fkDS, pkDS, fkCol, pkCol)
              val ncolJ = customMetadata(jDSStringID).ncols
              val ncolPk = customMetadata(pkDSStringID).ncols
              val ncolFk = customMetadata(fkDSStringID).ncols
              if (fkInPkContainment == 1.0 && !checkedJoins.contains(curJoinCandidate) && ncolJ == ncolPk + ncolFk - 1) { //todo: REMOVE schema FILTER IF PROJECTION IS ALLOWED
                val jHasEdgesToPKOrFKForAllNonNumericCols = graphContainsAllEdges(graph,customMetadata(jDSStringID),jDS,fkDS,pkDS)
                if(jHasEdgesToPKOrFKForAllNonNumericCols){
                  checkedJoins.add(curJoinCandidate)
                  checkedJoins.add((pkDS, fkDS, pkCol, fkCol)) //adding the reverse edge too
                  val pkDsLoaded = IOService.tryLoadAndCacheDataset(numericIdToDS(pkDS), version)
                  val fkDsLoaded = IOService.tryLoadAndCacheDataset(numericIdToDS(fkDS), version)
                  //val joinedDSKeepBoth = pkDsLoaded.join(fkDsLoaded, numericIdToCol((pkDS, pkCol)), numericIdToCol((fkDS, fkCol)), JoinConstructionVariant.KeepBoth)
                  val joinedDSKeepLeft = pkDsLoaded.join(fkDsLoaded, numericIdToCol((pkDS, pkCol)), numericIdToCol((fkDS, fkCol)), JoinConstructionVariant.KeepLeft)
                  //val joinedDSKeepRight = pkDsLoaded.join(fkDsLoaded, numericIdToCol((pkDS, pkCol)), numericIdToCol((fkDS, fkCol)), JoinConstructionVariant.KeepRight)
                  //discoveredJoins += findAndCheckJoinMatches(version, hashToDs, resultWriter, fkDSStringID, pkDSStringID, joinedDSKeepBoth)
                  discoveredJoins += findAndCheckJoinMatches(version, hashToDs, resultWriter, fkDSStringID, pkDSStringID, joinedDSKeepLeft)
                  //discoveredJoins += findAndCheckJoinMatches(version, hashToDs, resultWriter, fkDSStringID, pkDSStringID, joinedDSKeepRight)
                  executedJoinQueries += 1
                  logProgress("Join Queries", executedJoinQueries, 1000)
                }
                addToFilterAnalysis(filterAnalysisMap,"J to FK and PK full edge containment",jHasEdgesToPKOrFKForAllNonNumericCols)
              }
              //----------------Filter Analysis:
              addToFilterAnalysis(filterAnalysisMap,"fkInPkContainment",fkInPkContainment == 1.0)
              if(fkInPkContainment == 1.0)
                addToFilterAnalysis(filterAnalysisMap,"checkedJoin",!checkedJoins.contains(curJoinCandidate))
              if(fkInPkContainment == 1.0 && !checkedJoins.contains(curJoinCandidate))
                addToFilterAnalysis(filterAnalysisMap,"J.ncol == Pk.ncol + Fk.ncol - 1",ncolJ == ncolPk + ncolFk - 1)

              //-----------------------------------------
            }
            //----------------Filter Analysis:
            if(jDS!=pkDS)
              addToFilterAnalysis(filterAnalysisMap,"PK.pkCol.uniqueness=1.0",customMetadata(pkDSStringID).columnMetadata(pkColStringID).uniqueness == 1.0)
            //-----------------------------------------------
          }
        }
        //----------------Filter Analysis:
        addToFilterAnalysis(filterAnalysisMap,"jInFkContainment",jInFKContainment == 1.0)
        if(jInFKContainment == 1.0)
          addToFilterAnalysis(filterAnalysisMap,"fkInJContainment",fkInJContainment == 1.0)
        if(jInFKContainment == 1.0 && fkInJContainment == 1.0)
          addToFilterAnalysis(filterAnalysisMap,"J.nrow=FK.nrow",customMetadata(jDSStringID).nrows == customMetadata(fkDSStringID).nrows)
        //------------------------------------------------
        processedEdges += 1
        logProgress("Joinability Graph Edges", processedEdges, 10000,Some(numEdges))
        if (processedEdges % 10000 == 0) {
          printFilterPerformance(filterAnalysisMap)
          logger.debug(s"Found $discoveredJoins true join relationships")
        }
      }
    }
    logger.debug("finished program")
    printFilterPerformance(filterAnalysisMap)
    resultWriter.close()
  }

  private def findAndCheckJoinMatches(version: LocalDate, hashToDs: Map[Int, Iterable[CustomMetadata]], resultWriter: PrintWriter, fkDSStringID: String, pkDSStringID: String, joinedDSKeepBoth: LoadedRelationalDataset) = {
    val joinedHash = joinedDSKeepBoth.getTupleSpecificHash
    //find all candidates that have matching tuple specific hash values:
    var foundMatches = 0
    for (matchCandidate <- hashToDs.getOrElse(joinedHash, Seq())) {
      //validate Candidate:
      val candidateDs = IOService.tryLoadAndCacheDataset(matchCandidate.id, version)
      if(candidateDs.id!=fkDSStringID && candidateDs.id != pkDSStringID) {
        val mapping = getColumnMapping(candidateDs, joinedDSKeepBoth, None)
        if (mapping.isDefined && candidateDs.isEqualTo(joinedDSKeepBoth, mapping.get)) {
          resultWriter.println(s"${matchCandidate.id},$pkDSStringID,$fkDSStringID")
          resultWriter.flush()
          foundMatches += 1
        }
      }
    }
    foundMatches
  }


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
    if(from.columnMetadata.isEmpty) from.calculateColumnMetadata()
    if(to.columnMetadata.isEmpty) to.calculateColumnMetadata()
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
        .map(s => (s,from.columnMetadata(s).hash))
        .toMap
      if(freeInProjected.isEmpty)
        Some(columnMapping)
      else{
        val freeInOriginal = to.colNames.toSet.diff(columnMapping.values.toSet) //TODO: make the map the other way around here! - saves code later
          .map(s => (s,to.columnMetadata(s).hash))
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
    graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(version))
    logger.trace("finished graph construction")
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
        (id,newEdges,newEdges.map(_.src).toSet.size)
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
      projectedDS.calculateColumnMetadata()
      if(!projectedDS.isEmpty && projectedDS.ncols>1){
        for((srcID,edges,_) <- candidates) {
          val srcDSStringID = numericIdToDS(srcID)
          val originalDS = IOService.tryLoadDataset(srcDSStringID,version)
          IOService.cacheDataset(originalDS)
          originalDS.calculateColumnMetadata()
          val columnMapping = getColumnMapping(projectedDS,originalDS,Some(edges.map{case ColEdge(c1,c2) => (numericIdToCol((projectedID,c1)),numericIdToCol((srcID,c2)))}))
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

  case class FilterAnalysis(filterName:String, worksFor:String, var numFiltered:Long, var numCalled:Long) {
    def selectivity = (numCalled-numFiltered) / numCalled.toFloat

  }

}
