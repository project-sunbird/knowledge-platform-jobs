package org.sunbird.job.mvcindexer.util

import org.sunbird.job.exception.APIException
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil, JSONUtil}

object ContentUtil {

  /**
   * Get the Content Metadata and return the parsed metadata map
   * @param newmap Content metadata from event envelope
   * @param identifer Content ID
   * @param httpUtil HttpUil instance
   * @param config Config instance
   * @return parsed metadata map
   */
  @throws[Exception]
  def getContentMetaData(newmap: Map[String, AnyRef], identifer: String, httpUtil: HttpUtil, config: MVCIndexerConfig): Map[String, AnyRef] = {
    try {
      val contentReadURL = config.contentServiceBase
      val content: HTTPResponse = httpUtil.get(contentReadURL + "/content/v3/read/" + identifer)
      val obj = JSONUtil.deserialize[Map[String, AnyRef]](content.body)
      val contentObj = obj("result").asInstanceOf[Map[String, AnyRef]]("content").asInstanceOf[Map[String, AnyRef]]
      filterData(newmap, contentObj)
    } catch {
      case e: Exception =>
        throw new APIException(s"Error in getContentMetaData for $identifer :: ${e.getLocalizedMessage}", e)
    }
  }

  def filterData(obj: Map[String, AnyRef], content: Map[String, AnyRef]): Map[String, AnyRef] = {
    var contentObj = JSONUtil.deserialize[Map[String, AnyRef]](JSONUtil.serialize(obj))
    val elasticSearchParamSet = Set("organisation", "channel", "framework", "board", "medium", "subject", "gradeLevel", "name", "description", "language", "appId", "appIcon", "appIconLabel", "contentEncoding", "identifier", "node_id", "nodeType", "mimeType", "resourceType", "contentType", "allowedContentTypes", "objectType", "posterImage", "artifactUrl", "launchUrl", "previewUrl", "streamingUrl", "downloadUrl", "status", "pkgVersion", "source", "lastUpdatedOn", "ml_contentText", "ml_contentTextVector", "ml_Keywords", "level1Name", "level1Concept", "level2Name", "level2Concept", "level3Name", "level3Concept", "textbook_name", "sourceURL", "label", "all_fields")
    for (param <- elasticSearchParamSet) {
      val value = content.getOrElse(param, null)
      if (value != null) {
        contentObj += (param -> value)
      }
    }
    contentObj
  }
}
