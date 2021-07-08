package org.sunbird.job.mvcindexer.domain

import org.apache.commons.lang3.BooleanUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "MVCIndexer"

  def index: AnyRef = readOrDefault("index", null)

  def identifier: String = readOrDefault("object.id", "")

  var eventData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("eventData", Map[String, AnyRef]())

  def action: String = readOrDefault("eventData.action", "")

  def mlContentText: String = readOrDefault("eventData.ml_contentText", null)

  def mlKeywords: List[String] = readOrDefault("eventData.ml_Keywords", null)

  def metadata: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("eventData.metadata", Map[String, AnyRef]())

  def mlContentTextVector: List[Double] = {
    val mlContentTextVectorList = readOrDefault[List[List[Double]]]("eventData.ml_contentTextVector", null)

    if (mlContentTextVectorList != null) mlContentTextVectorList.head else null
  }

  def isValid: Boolean = {
    BooleanUtils.toBoolean(if (null == index) "true" else index.toString) && !eventMap.containsKey("edata") && eventData.nonEmpty
  }

}
