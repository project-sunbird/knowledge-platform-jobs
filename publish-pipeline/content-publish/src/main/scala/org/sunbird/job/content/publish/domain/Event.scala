package org.sunbird.job.content.publish.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.reader.JobRequest

import java.util
import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "content-publish"

  def eData: Map[String, AnyRef] = readOrDefault("edata", new util.HashMap[String, AnyRef]()).asScala.toMap

  def action: String = readOrDefault[String]("edata.action", "")

  def mimeType: String = readOrDefault[String]("edata.metadata.mimeType", "")

  def objectId: String = readOrDefault[String]("edata.metadata.identifier", "")

  def objectType: String = readOrDefault[String]("edata.metadata.objectType", "")

  def pkgVersion: Double = {
    val pkgVersion = readOrDefault[Double]("edata.metadata.pkgVersion", 0)
    pkgVersion
  }

  def validEvent(config: ContentPublishConfig): Boolean = {
    (StringUtils.equals("publish", action) && StringUtils.isNotBlank(objectId)) && (config.supportedObjectType.contains(objectType) && config.supportedMimeType.contains(mimeType))
  }
}