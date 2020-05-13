package de.hpi.dataset_versioning.experiment.example_query_imputation

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.custom.{ColumnDatatype, CustomMetadata, CustomMetadataCollection}
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata
import de.hpi.dataset_versioning.data.metadata.custom.joinability.{DatasetColumnVertex, JoinabilityGraph, JoinabilityGraphExplorer}
import de.hpi.dataset_versioning.data.LoadedRelationalDataset
import de.hpi.dataset_versioning.data.diff.RelationalDatasetDiff
import de.hpi.dataset_versioning.experiment.example_query_imputation.join.JoinConstructionVariant
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.util.TableFormatter

import scala.collection.mutable
import scala.io.Source

class QueryRelationshipDiscoverer() extends StrictLogging{

  val explorer = new JoinabilityGraphExplorer
  var graph:JoinabilityGraph = null
  logger.trace("Finished Projection Finder Initialization")

  def logProgress(thingToProcess:String,count: Int, modulo: Int, size: Option[Int] = None) = {
    if(count % modulo==0 && size.isDefined)
      logger.debug(s"processed $count out of ${size.get} $thingToProcess (${100*(count/size.get.toFloat)}%)")
    else if (count % modulo==0){
      logger.debug(s"processed $count $thingToProcess")
    }
  }

  def graphContainsAllEdges(graph: JoinabilityGraph,jMetadata:CustomMetadata, jVertex:DatasetColumnVertex, fkDS: Int, pkDS: Int): Boolean = {
    val it = jMetadata.columnMetadata.iterator
    var allContained = true
    while(it.hasNext){
      val (cname,cMetadata) = it.next()
      if(cMetadata.dataType!=ColumnDatatype.Numeric){
        val joinColID = jVertex.colID
        val colEdges = graph.adjacencyListGroupedByDSAndCol.getOrElse(jVertex, Map[DatasetColumnVertex, Float]())
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

  def readJoinCandidates(customMetadata:CustomMetadataCollection) = {
    Source.fromFile(IOService.getJoinCandidateFile()).getLines()
      .toIndexedSeq
      .tail
      .flatMap( s => {
        val tokens = s.split(",")
        val intID = tokens(0).toInt
        val version = LocalDate.parse(tokens(2), IOService.dateTimeFormatter)
        customMetadata.metadataByIntID(intID).columnMetadataByID.keySet
          .map(colID => DatasetColumnVertex(intID,version,colID))
      } )
  }

  /***
   * Current limitations: exactly two joined datasets only
   *
   * @param startVersion
   * @param endVersion
   * @param variant
   */
  def findProjectJoins(startVersion: LocalDate, endVersion:LocalDate) = {
    val mdStartVersion = LocalDate.parse("2019-11-01",IOService.dateTimeFormatter)
    val mdEndVersion = LocalDate.parse("2020-04-30",IOService.dateTimeFormatter)
    IOService.cacheCustomMetadata(mdStartVersion,mdEndVersion)
    val customMetadata = IOService.cachedCustomMetadata((mdStartVersion,mdEndVersion))
    logger.trace("Loading graph")
    graph = JoinabilityGraph.readGraphFromGoOutput(IOService.getJoinabilityGraphFile(startVersion,endVersion))
    graph.switchToAdjacencyListGroupedByDSAndCol()
    logger.trace("Done switching graph representation")
    val checkedJoins = mutable.HashSet[(Int,Int,Short,Short)]()
    val resultWriter = new PrintWriter(IOService.getInferredJoinFile(startVersion,endVersion))
    resultWriter.println("joinedDataset,joinedDsVersion,primaryKeyJoinPart,primaryKeyVersion,foreignKeyJoinPart,foreignKeyVersion")
    /*the following code uses these abbreviations:
    j - the candidate dataset for the join result
    fk - the candidate dataset for the relation containing the foreign key
    pk - the candidate dataset for the relation containing the primary key
    DS - Dataset
    Col - Column
     */
    var processedJoinCandidates = 0
    var executedJoinQueries = 0
    var discoveredJoins = 0
    val filterAnalysisMap = mutable.HashMap[String,FilterAnalysis]()
    filterAnalysisMap.put("jAfterFK",FilterAnalysis("jAfterFK","Join,Projection,Selection",0,0))
    filterAnalysisMap.put("jInFkContainment",FilterAnalysis("jInFkContainment","Join,Projection*",0,0))
    filterAnalysisMap.put("fkInJContainment",FilterAnalysis("fkInJContainment","Join,Projection*",0,0))
    filterAnalysisMap.put("J.nrow=FK.nrow",FilterAnalysis("J.nrow=FK.nrow","Join,Projection",0,0))
    filterAnalysisMap.put("jAfterPK",FilterAnalysis("jAfterPK","Join,Projection,Selection",0,0))
    filterAnalysisMap.put("PK.pkCol.uniqueness=1.0",FilterAnalysis("PK.pkCol.uniqueness=1.0","Join,Projection,Selection",0,0))
    filterAnalysisMap.put("fkInPkContainment",FilterAnalysis("fkInPkContainment","Join,Projection,Selection",0,0))
    filterAnalysisMap.put("checkedJoin",FilterAnalysis("checkedJoin","Join,Projection,Selection",0,0))
    //filterAnalysisMap.put("J.ncol == Pk.ncol + Fk.ncol - 1",FilterAnalysis("J.ncol == Pk.ncol + Fk.ncol - 1","Join,Selection",0,0))
    filterAnalysisMap.put("J to FK and PK full edge containment",FilterAnalysis("J to FK and PK full edge containment","Join,Projection,Selection",0,0))
    val sortedDSVersions = customMetadata.getSortedVersions
    val a = graph.adjacencyListGroupedByDSAndCol.exists{case (a,b) => b.exists{case (c,d) => a.version.isAfter(c.version)}}
    val joinCandidates = readJoinCandidates(customMetadata)
    for(jVertex <- joinCandidates){
      if(graph.adjacencyListGroupedByDSAndCol.contains(jVertex)){
        val (jDS,jVersion,jCol) = (jVertex.dsID,jVertex.version,jVertex.colID)
        //val jDSStringID = customMetadata.metadataByIntID(jDS).id
        for ((fkVertex, jInFKContainment) <- graph.adjacencyListGroupedByDSAndCol(jVertex)) {
          val (fkDS,fkVersion,fkCol) = (fkVertex.dsID,fkVertex.version,fkVertex.colID)
          val jInFKContainment = graph.getEdgeValue((jDS,jVersion),(fkDS,fkVersion),jCol,fkCol)
          if(jVersion.isAfter(fkVersion)) { //TODO: we can probably optimize this with smart filtering/indexing in the graph!
            val fkInJContainment = graph.getEdgeValue((fkDS,fkVersion), (jDS,jVersion), fkCol, jCol)
            if (jDS != fkDS && jInFKContainment == 1.0 && fkInJContainment == 1.0 && customMetadata.metadataByIntID(jDS).nrows == customMetadata.metadataByIntID(fkDS).nrows) { //todo: REMOVE NROW FILTER if selection is allowed
              //find primary key candidates:
              for ((DatasetColumnVertex(pkDS,pkVersion, pkCol), fkInPkContainment) <- graph.adjacencyListGroupedByDSAndCol(fkVertex)) {
                val jStrID = customMetadata.metadataByIntID(jDS).id
                val pkStrID = customMetadata.metadataByIntID(pkDS).id
                val fkStrID = customMetadata.metadataByIntID(fkDS).id
                if((sortedDSVersions(jStrID).head.intID != jDS || fkStrID == jStrID || pkStrID == jStrID ) && pkStrID!=fkStrID){
                  if (jVersion.isAfter(pkVersion) && jDS != pkDS && customMetadata.metadataByIntID(pkDS).columnMetadataByID(pkCol).uniqueness == 1.0) { //todo: RELAX THIS IF WE WANT TO ACCEPT DATA ERRORS
                    val curJoinCandidate = (fkDS, pkDS, fkCol, pkCol)
                    val ncolJ = customMetadata.metadataByIntID(jDS).ncols
                    val ncolPk = customMetadata.metadataByIntID(pkDS).ncols
                    val ncolFk = customMetadata.metadataByIntID(fkDS).ncols
                    if (fkInPkContainment == 1.0 && !checkedJoins.contains(curJoinCandidate)) {
                      val jHasEdgesToPKOrFKForAllNonNumericCols = graphContainsAllEdges(graph, customMetadata.metadataByIntID(jDS), jVertex, fkDS, pkDS)
                      if (jHasEdgesToPKOrFKForAllNonNumericCols) {
                        //checkedJoins.add(curJoinCandidate)
                        //checkedJoins.add((pkDS, fkDS, pkCol, fkCol)) //adding the reverse edge too
                        val pkDsLoaded = IOService.tryLoadAndCacheDataset(pkStrID, pkVersion,true)
                        val fkDsLoaded = IOService.tryLoadAndCacheDataset(fkStrID, fkVersion,true)
                        val jDSLoaded = IOService.tryLoadAndCacheDataset(customMetadata.metadataByIntID(jDS).id, jVersion,true)
                        //val joinedDSKeepBoth = pkDsLoaded.join(fkDsLoaded, numericIdToCol((pkDS, pkCol)), numericIdToCol((fkDS, fkCol)), JoinConstructionVariant.KeepBoth)
                        val joinedDSKeepBoth = pkDsLoaded.join(fkDsLoaded, pkCol, fkCol, JoinConstructionVariant.KeepBoth)
                        //val joinedDSKeepRight = pkDsLoaded.join(fkDsLoaded, numericIdToCol((pkDS, pkCol)), numericIdToCol((fkDS, fkCol)), JoinConstructionVariant.KeepRight)
                        //discoveredJoins += findAndCheckJoinMatches(version, hashToDs, resultWriter, fkDSStringID, pkDSStringID, joinedDSKeepBoth)
                        val fkDSStringID = fkStrID
                        val pkDSStringID = pkStrID
                        discoveredJoins += checkProjectJoinMatch(jDSLoaded,joinedDSKeepBoth,jVersion,fkVersion,pkVersion,fkDSStringID,pkDSStringID,resultWriter)
                        //discoveredJoins += findAndCheckJoinMatches(version, hashToDs, resultWriter, fkDSStringID, pkDSStringID, joinedDSKeepBoth)
                        //discoveredJoins += findAndCheckJoinMatches(version, hashToDs, resultWriter, fkDSStringID, pkDSStringID, joinedDSKeepRight)
                        executedJoinQueries += 1
                        logProgress("Join Queries", executedJoinQueries, 1000)
                      }
                      addToFilterAnalysis(filterAnalysisMap, "J to FK and PK full edge containment", jHasEdgesToPKOrFKForAllNonNumericCols)
                    }
                    //----------------Filter Analysis:
                    addToFilterAnalysis(filterAnalysisMap, "fkInPkContainment", fkInPkContainment == 1.0)
                    if (fkInPkContainment == 1.0)
                      addToFilterAnalysis(filterAnalysisMap, "checkedJoin", !checkedJoins.contains(curJoinCandidate))
                    //-----------------------------------------
                  }
                  //----------------Filter Analysis:
                  addToFilterAnalysis(filterAnalysisMap,"jAfterPK",jVersion.isAfter(pkVersion))
                  if (jVersion.isAfter(pkVersion) && jDS != pkDS)
                    addToFilterAnalysis(filterAnalysisMap, "PK.pkCol.uniqueness=1.0", customMetadata.metadataByIntID(pkDS).columnMetadataByID(pkCol).uniqueness == 1.0)
                  //-----------------------------------------------
                }
              }
            }
            addToFilterAnalysis(filterAnalysisMap,"jInFkContainment",jInFKContainment == 1.0)
            if(jInFKContainment == 1.0)
              addToFilterAnalysis(filterAnalysisMap,"fkInJContainment",fkInJContainment == 1.0)
            if(jInFKContainment == 1.0 && fkInJContainment == 1.0)
              addToFilterAnalysis(filterAnalysisMap,"J.nrow=FK.nrow",customMetadata.metadataByIntID(jDS).nrows == customMetadata.metadataByIntID(fkDS).nrows)
          }
          //----------------Filter Analysis:
          addToFilterAnalysis(filterAnalysisMap,"jAfterFK",jVersion.isAfter(fkVersion))
        }
      }
      //------------------------------------------------
      processedJoinCandidates += 1
      logProgress("Join Candidate Vertices", processedJoinCandidates, 1000,Some(joinCandidates.size))
      if (processedJoinCandidates % 1000 == 0) {
        printFilterPerformance(filterAnalysisMap)
        logger.debug(s"Found $discoveredJoins true join relationships")
      }
    }
    logger.debug("finished program")
    printFilterPerformance(filterAnalysisMap)
    resultWriter.close()
  }

  private def checkProjectJoinMatch(candidateDs: LoadedRelationalDataset, joinResult: LoadedRelationalDataset,jVersion:LocalDate,fkVersion:LocalDate,pkVersion:LocalDate,pkDSStringID:String,fkDSStringID:String, resultWriter: PrintWriter) = {
    val mapping = RelationalDatasetDiff.getColumnMapping(candidateDs, joinResult, None)
    if (mapping.isDefined && candidateDs.isProjectionOf(joinResult, mapping.get)) {
      resultWriter.println(s"${candidateDs.id},$jVersion,$pkDSStringID,$pkVersion,$fkDSStringID,$fkVersion")
      resultWriter.flush()
      1
    } else
      0


  }

  /*private def findAndCheckJoinMatches(version: LocalDate, hashToDs: Map[Int, Iterable[CustomMetadata]], resultWriter: PrintWriter, fkDSStringID: String, pkDSStringID: String, joinedDSKeepBoth: LoadedRelationalDataset) = {
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
  }*/


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

/*
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
  }*/

  case class FilterAnalysis(filterName:String, worksFor:String, var numFiltered:Long, var numCalled:Long) {
    def selectivity = (numCalled-numFiltered) / numCalled.toFloat

  }

}
