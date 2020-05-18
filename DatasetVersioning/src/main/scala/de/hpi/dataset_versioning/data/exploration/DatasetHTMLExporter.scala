package de.hpi.dataset_versioning.data.exploration

import java.io.{File, PrintWriter}

import com.google.gson.JsonElement
import de.hpi.dataset_versioning.data.LoadedRelationalDataset
import de.hpi.dataset_versioning.data.diff.semantic.RelationalDatasetDiff
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.io.Source

class DatasetHTMLExporter() {

  val idToLineageMap = IOService.readCleanedDatasetLineages()
    .map(l => (l.id,l))
    .toMap

  def exportDiffMetadataToHTMLPrinter(ds1: LoadedRelationalDataset,ds2:LoadedRelationalDataset, pr: PrintWriter) = {
    IOService.cacheMetadata(ds1.version)
    IOService.cacheMetadata(ds2.version)
    val md1 = IOService.cachedMetadata(ds1.version)(ds1.id)
    val md2 = IOService.cachedMetadata(ds2.version)(ds2.id)
    //opening Div:
    pr.println("<p><div style=\"width:100%;overflow:auto;height:200px\">")
    //ID and lineage size
    pr.println("<b>ID: </b>")
    pr.println(escapeHTML(s"${ds1.id}"))
    pr.println("&emsp;")
    pr.println("<b>#Changes: </b>")
    pr.println(escapeHTML(s"${idToLineageMap(ds1.id).versionsWithChanges.size}"))
    pr.println("&emsp;")
    pr.println("<b>#Deletions: </b>")
    pr.println(escapeHTML(s"${idToLineageMap(ds1.id).deletions.size}"))
    pr.println("<br/>")
    //Version:
    //pr.println("<p><div style=\"width:100%;overflow:auto;\">")
    pr.println("<b>Version: </b>")
    pr.println(escapeHTML(s"${ds1.version.format(IOService.dateTimeFormatter)} --> ${ds2.version.format(IOService.dateTimeFormatter)}"))
    pr.println("<br/>")
    //Name
    val nameStr = if(md1.resource.name == md2.resource.name) md1.resource.name else s"${md1.resource.name} --> ${md2.resource.name}"
    //pr.println("<p><div style=\"width:100%;overflow:auto;\">")
    pr.println("<b>Name: </b>")
    pr.println(escapeHTML(s"$nameStr"))
    pr.println("<br/>")
    //Link
    val linkStr = if(md1.link == md2.link) md1.link else s"${md1.link} --> ${md2.link}"
    //pr.println("<p><div style=\"width:100%;overflow:auto;\">")
    pr.println("<b>Link: </b>")
    pr.println(escapeHTML(s"$linkStr"))
    pr.println("<br/>")
    val descrStr = if(md1.resource.description.getOrElse("-") == md2.resource.description.getOrElse("-")) md1.resource.description.getOrElse("-") else s"${md1.resource.description.getOrElse("-")} --> ${md2.resource.description.getOrElse("-")}"
    //pr.println("<p><div style=\"width:100%;overflow:auto;\">")
    pr.println("<b>Description: </b>")
    pr.println(escapeHTML(s"$descrStr"))
    pr.println("<br/>")
    //closing div
    pr.println("</div></p>")
  }

  def exportDiffToHTMLPrinter(dsBeforeChange: LoadedRelationalDataset, dsAfterChange: LoadedRelationalDataset, diff: RelationalDatasetDiff, pr: PrintWriter) = {
    //very simple schema based col-matching
    val colDeletes = dsBeforeChange.colNames.toSet.diff(dsAfterChange.colNames.toSet)
    val colInserts = dsAfterChange.colNames.toSet.diff(dsBeforeChange.colNames.toSet)
    val colMatches = dsBeforeChange.colNames.toSet.intersect(dsAfterChange.colNames.toSet)
    val resultColSet = dsBeforeChange.colNames.toSet.union(dsAfterChange.colNames.toSet)
        .toIndexedSeq
        .sorted
    val diffStyleDelete = "<div style=\"color:red;text-decoration:line-through;resize:none;\">"
    val diffStyleInsert = "<div style=\"color:green;resize:none;\">"
    val diffStyleUpdate = "<div style=\"color:blue;resize:none;\">"
    val diffStyleNormal = "<div style=\"resize:none;\">"
    pr.println("<tr>")
    resultColSet.foreach(cell => {
      if(colMatches.contains(cell)) {
        pr.println("<th><div>")
        pr.println(escapeHTML(cell))
        pr.println("</div></th>")
      } else if(colDeletes.contains(cell)){
        pr.println("<th>" + diffStyleDelete)
        pr.println(escapeHTML(cell))
        pr.println("</div></th>")
      } else{
        assert(colInserts.contains(cell))
        pr.println("<th><div style=\"color:green;\">")
        pr.println(escapeHTML(cell))
        pr.println("</div></th>")
      }
    })
    pr.println("</tr>")
    val resultingColOrder = resultColSet.zipWithIndex.toMap
    //TODO: think about tuple order?
    //deleted first! - then inserts - then updates
    addToHTMLTable(diff.deletes, diffStyleDelete, resultingColOrder,pr)
    addToHTMLTable(diff.inserts, diffStyleInsert, resultingColOrder,pr)
    addUpdatedTuplesToHTMLTable(diff.updates, diffStyleUpdate,diffStyleInsert,diffStyleDelete,diffStyleNormal,resultingColOrder,pr)
    addToHTMLTable(diff.unchanged,diffStyleNormal,resultingColOrder,pr)

  }

  def addUpdatedTuplesToHTMLTable(updates: mutable.HashMap[Set[(String, JsonElement)], Set[(String, JsonElement)]],
                                  diffStyleUpdate: String,diffStyleInsert:String,diffStyleDelete:String,
                                  diffStyleNormal:String, resultingColOrder: Map[String, Int], pr: PrintWriter) = {
    updates.foreach { case(from,to) => {
      val fromMap = from.toMap
      val toMap = to.toMap
      pr.println("<tr>")
      val htmlCells = mutable.ArrayBuffer[String]() ++= Seq.fill[String](resultingColOrder.size)("-")
      resultingColOrder.foreach { case (cname, curCellIndex) => {
        var curCell = "-"
        if(fromMap.contains(cname) && toMap.contains(cname)){
          //we have an update:
          val prevValue = fromMap(cname)
          val newValue = toMap(cname)
          if(prevValue != newValue)
            curCell = "<td>" + diffStyleUpdate + escapeHTML(LoadedRelationalDataset.getCellValueAsString(prevValue) + " --> " + LoadedRelationalDataset.getCellValueAsString(newValue)) + "</div></td>"
          else
            curCell = "<td>" + diffStyleNormal + escapeHTML(LoadedRelationalDataset.getCellValueAsString(prevValue)) + "</div></td>"
        } else if(fromMap.contains(cname)){
          val prevValue = fromMap(cname)
          curCell = "<td>" + diffStyleDelete + escapeHTML(LoadedRelationalDataset.getCellValueAsString(prevValue)) + "</div></td>"
        } else{
          assert(toMap.contains(cname))
          val newValue = toMap(cname)
          curCell = "<td>" + diffStyleInsert + escapeHTML(LoadedRelationalDataset.getCellValueAsString(newValue)) + "</div></td>"
        }
        htmlCells(curCellIndex) = curCell
      }
      }
      htmlCells.foreach(c => {
        pr.println(c)
      })
      pr.println("</tr>")
    }
    }

  }

  private def addToHTMLTable(rows:  mutable.HashSet[Set[(String, JsonElement)]], diffStyle: String, resultingColOrder: Map[String, Int], pr:PrintWriter) = {
    rows.foreach { tupleSet => {
      pr.println("<tr>")
      val htmlCells = mutable.ArrayBuffer[String]() ++= Seq.fill[String](resultingColOrder.size)("-")
      tupleSet.foreach { case (cname, e) => {
        val curCellIndex = resultingColOrder(cname)
        val curCell = "<td>" + diffStyle + escapeHTML(LoadedRelationalDataset.getCellValueAsString(e)) + "</div></td>"
        htmlCells(curCellIndex) = curCell
      }
      }
      htmlCells.foreach(c => {
        pr.println(c)
      })
      pr.println("</tr>")
    }
    }
  }

  def exportDiffTableView(dsBeforeChange: LoadedRelationalDataset, dsAfterChange: LoadedRelationalDataset, diff: RelationalDatasetDiff, outFile: File) = {
    val template = "/html_output_templates/ScollableTableTemplate.html"
    val pr = new PrintWriter(outFile)
    val is = getClass.getResourceAsStream(template)
    Source.fromInputStream(is)
      .getLines()
      .foreach( l => {
        if(l.startsWith("????")){
          if(l.contains("METAINFO"))
            exportDiffMetadataToHTMLPrinter(dsBeforeChange,dsAfterChange,pr)
          else if(l.contains("TABLECONTENT"))
            exportDiffToHTMLPrinter(dsBeforeChange,dsAfterChange,diff,pr)
        } else
          pr.println(l)
      })
    pr.close()
  }

  def exportCorrelationInfoToHTMLPrinter(dp: ChangeCorrelationInfo, pr: PrintWriter) = {
    pr.println("<b>A: </b>")
    pr.println(escapeHTML(s"${dp.idA}"))
    pr.println("<b>P(A): </b>")
    pr.println(escapeHTML(s"${dp.P_A}"))
    pr.println("&emsp;")
    pr.println("<b>B: </b>")
    pr.println(escapeHTML(s"${dp.idB}"))
    pr.println("<b>P(B): </b>")
    pr.println(escapeHTML(s"${dp.P_B}"))
    pr.println("<br>")
    pr.println("<b>(B AND B): </b>")
    pr.println(escapeHTML(s"${dp.P_A_AND_B}"))
    pr.println("&emsp;")
    pr.println("<b>P(A|B): </b>")
    pr.println(escapeHTML(s"${dp.P_A_IF_B}"))
    pr.println("&emsp;")
    pr.println("<b>P(B|A): </b>")
    pr.println(escapeHTML(s"${dp.P_B_IF_A}"))
    pr.println("<br>")

  }

  def exportDiffPairToTableView(dsABeforeChange: LoadedRelationalDataset,
                                dsAAfterChange: LoadedRelationalDataset,
                                diffA: RelationalDatasetDiff,
                                dsBBeforeChange: LoadedRelationalDataset,
                                dsBAfterChange: LoadedRelationalDataset,
                                diffB: RelationalDatasetDiff,
                                dp:ChangeCorrelationInfo,
                                outFile: File) = {
    val template = "/html_output_templates/ScrollableTableTemplateForJointDiff.html"
    val pr = new PrintWriter(outFile)
    val is = getClass.getResourceAsStream(template)
    Source.fromInputStream(is)
      .getLines()
      .foreach( l => {
        if(l.trim.startsWith("????")){
          if(l.contains("CORRELATIONMETAINFO"))
            exportCorrelationInfoToHTMLPrinter(dp,pr)
          if(l.contains("METAINFO1"))
            exportDiffMetadataToHTMLPrinter(dsABeforeChange,dsAAfterChange,pr)
          else if(l.contains("TABLECONTENT1"))
            exportDiffToHTMLPrinter(dsABeforeChange,dsAAfterChange,diffA,pr)
          if(l.contains("METAINFO2"))
            exportDiffMetadataToHTMLPrinter(dsBBeforeChange,dsBAfterChange,pr)
          else if(l.contains("TABLECONTENT2"))
            exportDiffToHTMLPrinter(dsBBeforeChange,dsBAfterChange,diffB,pr)
        } else
          pr.println(l)
      })
    pr.close()
  }


  def escapeHTML(s:String): String = {
    val sb = new mutable.StringBuilder()
    s.foreach(c => {
      c match {
        case '&' => sb.append("&amp;")
        case '<' => sb.append("&lt;")
        case '>' => sb.append("&gt;")
        case '"' => sb.append("&quot;")
        case ''' => sb.append("&#39;")
        case c => sb.append(c)
      }
    })
    sb.toString()
  }

  def toHTMLTableRow(row: IndexedSeq[Any], cellTag:String,useDiv:Boolean) = {
    val divStrOpen = if(useDiv) "<div>" else ""
    val divStrClose = if(useDiv) "</div>" else ""
    s"<tr>\n<$cellTag>$divStrOpen${row.map(c => escapeHTML(c.toString)).mkString(s"$divStrClose</$cellTag>\n<$cellTag>$divStrOpen")}$divStrClose</$cellTag>\n</tr>"
  }

  def exportToHTMLPrinter(ds:LoadedRelationalDataset,pr: PrintWriter) = {
    pr.println(toHTMLTableRow(ds.colNames,"th",true))
    ds.rows.foreach(r => {
      pr.println(toHTMLTableRow(r,"td",false))
    })
  }

  def exportMetadataToHTMLPrinter(ds: LoadedRelationalDataset, pr: PrintWriter) = {
    IOService.cacheMetadata(ds.version)
    val md = IOService.cachedMetadata(ds.version)(ds.id)
    pr.println("<p><div style=\"width:100%;overflow:auto;\">" + escapeHTML(s"ID: ${ds.id}") + "<br/>" +
      escapeHTML(s"Version: ${ds.version.format(IOService.dateTimeFormatter)}")+ "<br/>" +
      escapeHTML(s"Name: ${md.resource.name}")+ "<br/>" +
      escapeHTML(s"URL: ${md.link}") + "<br/>" +
      escapeHTML(s"Description: ${md.resource.description.getOrElse("-")}") + "</p></div>")
  }

  def toHTML(ds:LoadedRelationalDataset, outFile:File) = {
    val template = "src/main/resources/html_output_templates/ScollableTableTemplate.html"
    val pr = new PrintWriter(outFile)
    Source.fromFile(template)
      .getLines()
      .foreach( l => {
        if(l.startsWith("????")){
          if(l.contains("METAINFO"))
            exportMetadataToHTMLPrinter(ds,pr)
          else if(l.contains("TABLECONTENT"))
            exportToHTMLPrinter(ds,pr)
        } else
          pr.println(l)
      })
    pr.close()
  }

}
