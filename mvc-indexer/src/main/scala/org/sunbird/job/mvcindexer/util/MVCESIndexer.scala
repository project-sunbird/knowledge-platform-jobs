package org.sunbird.job.mvcindexer.util

import org.slf4j.LoggerFactory
import org.sunbird.job.exception.ElasticSearchException
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.util.{ElasticSearchUtil, JSONUtil}

import java.io.IOException


class MVCESIndexer(config: MVCIndexerConfig, esUtil: ElasticSearchUtil) {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MVCESIndexer])
  private val NESTED_FIELDS = config.nestedFields

  /**
   * Create mvc index in Elasticsearch if not available
   */
  @throws[ElasticSearchException]
  def createMVCSearchIndex(): Unit = {
    val alias = "mvc-content"
    val settings = """{"max_ngram_diff":"29","mapping":{"total_fields":{"limit":"1500"}},"number_of_shards":"5","analysis":{"filter":{"mynGram":{"token_chars":["letter","digit","whitespace","punctuation","symbol"],"min_gram":"1","type":"nGram","max_gram":"30"}},"analyzer":{"cs_index_analyzer":{"filter":["lowercase","mynGram"],"type":"custom","tokenizer":"standard"},"keylower":{"filter":"lowercase","tokenizer":"keyword"},"ml_custom_analyzer":{"type":"standard","stopwords":["_english_","_hindi_"]},"cs_search_analyzer":{"filter":["lowercase"],"type":"custom","tokenizer":"standard"}}},"number_of_replicas":"1"}"""
    val mappings = """{"dynamic":"strict","properties":{"all_fields":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"allowedContentTypes":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"appIcon":{"type":"text","index":false},"appIconLabel":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"appId":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"artifactUrl":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"board":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"channel":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"contentEncoding":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"contentType":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"description":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"downloadUrl":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"framework":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"gradeLevel":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"identifier":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"label":{"type":"text","analyzer":"cs_index_analyzer","search_analyzer":"standard"},"language":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"lastUpdatedOn":{"type":"date","fields":{"raw":{"type":"date"}}},"launchUrl":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level1Concept":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level1Name":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level2Concept":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level2Name":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level3Concept":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"level3Name":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"medium":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"mimeType":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"ml_Keywords":{"type":"text","analyzer":"ml_custom_analyzer","search_analyzer":"standard"},"ml_contentText":{"type":"text","analyzer":"ml_custom_analyzer","search_analyzer":"standard"},"ml_contentTextVector":{"type":"long"},"name":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"nodeType":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"node_id":{"type":"long","fields":{"raw":{"type":"long"}}},"objectType":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"organisation":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"pkgVersion":{"type":"double","fields":{"raw":{"type":"double"}}},"posterImage":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"previewUrl":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"resourceType":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"source":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"sourceURL":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"status":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"streamingUrl":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"subject":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"},"textbook_name":{"type":"text","fields":{"raw":{"type":"text","analyzer":"keylower","fielddata":true}},"copy_to":["all_fields"],"analyzer":"cs_index_analyzer","search_analyzer":"cs_search_analyzer"}}}"""
    try {
      esUtil.addIndex(settings, mappings, alias)
    } catch {
      case e: Exception =>
        throw new ElasticSearchException(s"Exception while creating index into in Elasticsearch :: ${e.getMessage}", e)
    }

  }

  /**
   * Insert or update the document in ES based on the action. content id
   * @param uniqueId Content ID
   * @param message Event envelope
   */
  @throws[ElasticSearchException]
  def upsertDocument(uniqueId: String, message: Event): Unit = {
    try {
      var jsonIndexDocument: Map[String, AnyRef] = removeExtraParams(message.eventData)
      jsonIndexDocument ++= proocessNestedProps(jsonIndexDocument)
      var jsonAsString = JSONUtil.serialize(jsonIndexDocument)
      message.action match {
        case "update-es-index" =>
          esUtil.addDocumentWithIndex(jsonAsString, config.mvcProcessorIndex, uniqueId)

        case "update-content-rating" =>
          val resp = esUtil.getDocumentAsString(uniqueId)
          if (null != resp && resp.contains(uniqueId)) {
            logger.info("ES Document Found With Identifier " + uniqueId + " | Updating Content Rating.")
            esUtil.updateDocument(uniqueId, JSONUtil.serialize(message.metadata))
          }
          else logger.info("ES Document Not Found With Identifier " + uniqueId + " | Skipped Updating Content Rating.")

        case "update-ml-contenttextvector" =>
          jsonIndexDocument += ("ml_contentTextVector" -> message.mlContentTextVector)
          jsonAsString = JSONUtil.serialize(jsonIndexDocument)
          esUtil.updateDocument(uniqueId, jsonAsString)

        case "update-ml-keywords" =>
          esUtil.updateDocument(uniqueId, jsonAsString)
      }
    } catch {
      case e: Exception =>
        throw new ElasticSearchException(s"Exception while inserting data into ES for $uniqueId :: ${e.getMessage}", e)
    }
  }

  /**
   * Updating nested type props as null in ES
   * @param jsonIndexDocument Content metadata
   * @return New map with available nested props as null
   */
  @throws[IOException]
  private def proocessNestedProps(jsonIndexDocument: Map[String, AnyRef]): Map[String, AnyRef] = {
    var nestedProps = Map[String, AnyRef]()
    for (propertyName <- jsonIndexDocument.keySet) {
      if (NESTED_FIELDS.contains(propertyName)) {
        val propertyNewValue = Map[String, AnyRef](jsonIndexDocument.get(propertyName).asInstanceOf[String] -> null)
        nestedProps += (propertyName -> propertyNewValue)
      }
    }
    nestedProps
  }

  /**
   * Remove params which should not be inserted into ES from content metadata
   */
  def removeExtraParams(obj: Map[String, AnyRef]): Map[String, AnyRef] = {
    obj.-("action", "stage")
  }
}
