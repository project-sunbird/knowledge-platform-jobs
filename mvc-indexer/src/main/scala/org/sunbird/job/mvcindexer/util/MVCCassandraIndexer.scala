package org.sunbird.job.mvcindexer.util

import com.datastax.driver.core.querybuilder.Update.Assignments
import com.datastax.driver.core.querybuilder.{QueryBuilder, Update}
import org.slf4j.LoggerFactory
import org.sunbird.job.exception.{APIException, CassandraException}
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil}

class MVCCassandraIndexer(config: MVCIndexerConfig, cassandraUtil: CassandraUtil, httpUtil: HttpUtil) {
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[MVCCassandraIndexer])

  /**
   * Based on action update content metadata in cassandra and post the keywords and vector data to ML Service
   * @param message Event envelope
   * @param identifier Content ID
   */
  def insertIntoCassandra(message: Event, identifier: String): Unit = {
    val obj: Map[String, AnyRef] = message.eventData
    message.action match {
      case "update-es-index" =>
        val esCassandraMap = extractFieldsToBeInserted(obj)
        getMLKeywords(obj)
        updateContentProperties(identifier, esCassandraMap)
      case "update-ml-keywords" =>
        getMLVectors(message.mlContentText, identifier)
        val csTableCols = Map[String, AnyRef]("ml_keywords" -> message.mlKeywords, "ml_content_text" -> message.mlContentText)
        updateContentProperties(identifier, csTableCols)
      case "update-ml-contenttextvector" =>
        val vectorSet = JSONUtil.deserialize[java.util.HashSet[java.lang.Double]](JSONUtil.serialize(message.mlContentTextVector))
        val csTableCols = Map[String, AnyRef]("ml_content_text_vector" -> vectorSet)
        updateContentProperties(identifier, csTableCols)
    }
  }

  /**
   * Parse the selected fields from eventData to update in cassandra
   * @param contentobj Content metadata
   */
  private def extractFieldsToBeInserted(contentobj: Map[String, AnyRef]): Map[String, AnyRef] = {
    var esCassandraMap = Map[String, AnyRef]()
    val fields = Map[String, String]("level1Concept" -> "level1_concept", "level2Concept" -> "level2_concept",
      "level3Concept" -> "level3_concept", "textbook_name" -> "textbook_name", "level1Name" -> "level1_name",
      "level2Name" -> "level2_name", "level3Name" -> "level3_name")
    for ((fieldKey: String, fieldValue: String) <- fields) {
      if (contentobj.contains(fieldKey)) {
        esCassandraMap += (fieldValue -> contentobj(fieldKey).asInstanceOf[List[String]])
      }
    }

    if (contentobj.contains("source") && contentobj("source").isInstanceOf[List[String]]) {
      esCassandraMap += ("source" -> contentobj("source").asInstanceOf[List[String]].head)
    }
    else if (contentobj.contains("source") && contentobj("source").isInstanceOf[String]) {
      esCassandraMap += ("source" -> contentobj("source"))
    }
    if (contentobj.contains("sourceURL")) esCassandraMap += ("sourceurl" -> contentobj("sourceURL"))

    esCassandraMap
  }

  /**
   * Post the keywords to ML Workbench service
   * @param contentdef Content Metadata
   */
  @throws[APIException]
  private def getMLKeywords(contentdef: Map[String, AnyRef]): Unit = {
    val bodyObj = Map("request" -> Map("job" -> config.keywordAPIJobname, "input" -> Map("content" -> List(contentdef))))

    val requestBody = JSONUtil.serialize(bodyObj)
    try {
      val resp: HTTPResponse = httpUtil.post(config.mlKeywordAPIUrl, requestBody)
      logger.info("getMLKeywords ::: The ML workbench response is " + resp.body)
      if(!resp.isSuccess) throw new Exception("")
    } catch {
      case e: Exception =>
        throw new APIException(s"getMLKeywords ::: ML workbench api request failed :: ${e.getMessage}", e)
    }
  }

  /**
   * Post the content text vector to ML Workbench service
   * @param contentText ContentText from event envelope
   * @param identifier Content ID
   */
  @throws[APIException]
  def getMLVectors(contentText: String, identifier: String): Unit = {
    val bodyObj = Map("request" -> Map("language" -> "en", "method" -> "BERT", "params" -> Map("dim" -> 768, "seq_len" -> 25), "cid" -> identifier, "text" -> List(contentText)))

    val requestBody = JSONUtil.serialize(bodyObj)
    try {
      val resp: HTTPResponse = httpUtil.post(config.mlVectorAPIUrl, requestBody)
      logger.info("getMLVectors ::: ML vector api request response is " + resp.body)
      if(!resp.isSuccess) throw new Exception("")
    } catch {
      case e: Exception =>
        throw new APIException(s"getMLVectors ::: ML vector api failed for $identifier :: ${e.getMessage}", e)
    }
  }

  @throws[CassandraException]
  def updateContentProperties(contentId: String, map: Map[String, AnyRef]): Unit = {
    if (Option(map).forall(_.isEmpty)) return
    import scala.collection.JavaConverters._

    try {
      val query: Update = QueryBuilder.update(config.dbKeyspace, config.dbTable)
      var queryAssignments: Assignments = null
      var i = 0
      for ((key, value) <- map.toList) {
        if (null != value && null != key) {
          val querySet = value match {
            case strings: List[String] =>
              QueryBuilder.set(key, strings.asJava)
            case _ =>
              QueryBuilder.set(key, value)
          }

          queryAssignments = if (i == 0) {
            query.`with`(querySet)
          } else {
            queryAssignments.and(querySet)
          }
        } else {
          return
        }
        i += 1
      }

      queryAssignments.and(QueryBuilder.set("last_updated_on", System.currentTimeMillis))
      val finalQuery = queryAssignments.where(QueryBuilder.eq("content_id", contentId))
      cassandraUtil.session.execute(finalQuery.toString)
    } catch {
      case e: Exception =>
        throw new CassandraException(s"Exception while inserting data into cassandra for $contentId :: ${e.getMessage}", e)
    }
  }
}
