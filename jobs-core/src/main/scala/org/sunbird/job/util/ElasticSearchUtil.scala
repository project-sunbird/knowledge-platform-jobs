package org.sunbird.job.util

import java.io.IOException
import java.util
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

class ElasticSearchUtil(connectionInfo: String, indexName: String, indexType: String, batchSize: Int = 1000) extends Serializable {

  private val resultLimit = 100
  private val esClient: RestHighLevelClient = createClient(connectionInfo)
  private val mapper = new ObjectMapper

  private[this] val logger = LoggerFactory.getLogger(classOf[ElasticSearchUtil])

  System.setProperty("es.set.netty.runtime.available.processors", "false")

  private def createClient(connectionInfo: String): RestHighLevelClient = {
    val httpHosts: List[HttpHost] = connectionInfo.split(",").map(info => {
      val host = info.split(":")(0)
      val port = info.split(":")(1).toInt
      new HttpHost(host, port)
    }).toList

    val builder: RestClientBuilder = RestClient.builder(httpHosts: _*).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
      override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder = {
        requestConfigBuilder.setConnectionRequestTimeout(-1)
      }
    })
    new RestHighLevelClient(builder)
  }

  def isIndexExists(): Boolean = {
    try {
      val response = esClient.getLowLevelClient.performRequest("HEAD", "/" + indexName)
      response.getStatusLine.getStatusCode == 200
    } catch {
      case e: IOException => {
        logger.error("Failed to check Index if Present or not. Exception : ", e)
        false
      }
    }
  }

  def addIndex(settings: String, mappings: String, alias: String = ""): Boolean = {
    var response = false
    val client = esClient
    if (!isIndexExists()) {
      val createRequest = new CreateIndexRequest(indexName)
      if (StringUtils.isNotBlank(alias)) createRequest.alias(new Alias(alias))
      if (StringUtils.isNotBlank(settings)) createRequest.settings(Settings.builder.loadFromSource(settings, XContentType.JSON))
      if (StringUtils.isNotBlank(indexType) && StringUtils.isNotBlank(mappings)) createRequest.mapping(indexType, mappings, XContentType.JSON)
      val createIndexResponse = client.indices.create(createRequest)
      response = createIndexResponse.isAcknowledged
    }
    response
  }

  def addDocument(identifier: String, document: String): Unit = {
    try {
      // TODO
      // Replace mapper with JSONUtil once the JSONUtil is fixed
      val doc = mapper.readValue(document, new TypeReference[util.Map[String, AnyRef]]() {})
      val response = esClient.index(new IndexRequest(indexName, indexType, identifier).source(doc))
      logger.info(s"Added ${response.getId} to index ${response.getIndex}")
    } catch {
      case e: IOException =>
        logger.error(s"Error while adding document to index : $indexName", e)
    }
  }

  @throws[IOException]
  def addDocumentWithIndex(document: String, indexName: String, identifier: String = null): Unit = {
    try {
      // TODO
      // Replace mapper with JSONUtil once the JSONUtil is fixed
      val doc = mapper.readValue(document, new TypeReference[util.Map[String, AnyRef]]() {})
      val indexRequest = if(identifier == null) new IndexRequest(indexName, indexType) else new IndexRequest(indexName, indexType, identifier)
      val response = esClient.index(indexRequest.source(doc))
      logger.info(s"Added ${response.getId} to index ${response.getIndex}")
    } catch {
      case e: IOException =>
        logger.error(s"Error while adding document to index : $indexName : " + e.getMessage)
        e.printStackTrace()
        throw e
    }
  }

  def updateDocument(identifier: String, document: String): Unit = {
    try {
      // TODO
      // Replace mapper with JSONUtil once the JSONUtil is fixed
      val doc = mapper.readValue(document, new TypeReference[util.Map[String, AnyRef]]() {})
      val indexRequest = new IndexRequest(indexName, indexType, identifier).source(doc)
      val request = new UpdateRequest().index(indexName).`type`(indexType).id(identifier).doc(doc).upsert(indexRequest)
      val response = esClient.update(request)
      logger.info(s"Updated ${response.getId} to index ${response.getIndex}")
    } catch {
      case e: IOException =>
        logger.error(s"Error while updating document to index : $indexName", e)
    }
  }

  def deleteDocument(identifier: String): Unit = {
    val response = esClient.delete(new DeleteRequest(indexName, indexType, identifier))
    logger.info(s"Deleted ${response.getId} to index ${response.getIndex}")
  }

  def getDocumentAsString(identifier: String): String = {
    val response = esClient.get(new GetRequest(indexName, indexType, identifier))
    response.getSourceAsString
  }

  def close(): Unit = {
    if (null != esClient) try esClient.close()
    catch {
      case e: IOException => e.printStackTrace()
    }
  }

}
