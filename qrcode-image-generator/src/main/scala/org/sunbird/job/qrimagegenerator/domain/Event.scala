package org.sunbird.job.qrimagegenerator.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.qrimagegenerator.functions.Config
import org.sunbird.job.util.JSONUtil

class Event (eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "qrcode-image-generator"

  def eid = read[String]("eid").get

  def processId = readOrDefault[String]("processId", "")

  def objectId = readOrDefault[String]("objectId", "")

  def storageContainer = readOrDefault[String]("storage.container", "")

  def storagePath = readOrDefault[String]("storage.path", "")

  def storageFileName = readOrDefault[String]("storage.fileName", "")

  def imageConfig = JSONUtil.deserialize[Config](JSONUtil.serialize(readOrDefault[Map[String, AnyRef]]("config", Map())))

  def dialCodes = readOrDefault[List[Map[String, AnyRef]]]("dialcodes", List())

  def imageFormat = imageConfig.imageFormat.getOrElse("png")

  def isValid(): Boolean = {
    ( null != eid ) && ( !eid.isEmpty ) && ( StringUtils.equals("BE_QR_IMAGE_GENERATOR", eid))
  }

  def isValidDialcodes = {
    (null != dialCodes) && (dialCodes.size > 0)
  }
}
