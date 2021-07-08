package org.sunbird.job.mvcindexer.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig

class MVCIndexerConfig(override val config: Config) extends BaseJobConfig(config, "mvc-indexer") {

  private val serialVersionUID = 2905979434303791379L

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val esFailedEventCount = "elasticsearch-failed-events-count"
  val skippedEventCount = "skipped-events-count"
  val dbUpdateCount = "db-update-count"
  val dbUpdateFailedCount = "db-update-failed-count"
  val apiFailedEventCount = "api-failed-events-count"

  // Consumers
  val eventConsumer = "mvc-indexer-consumer"
  val mvcIndexerFunction = "mvc-indexer-function"

  val configVersion = "1.0"

  // ES Configs
  val esConnectionInfo = config.getString("es.basePath")
  val nestedFields = List(config.getString("nested.fields").split(","))
  val mvcProcessorIndex = "mvc-content-v1"
  val mvcProcessorIndexType = "_doc"
  val operationCreate = "CREATE"

  // API Configs
  val contentServiceBase = config.getString("learning_service.basePath")
  val mlVectorAPIUrl = s"http://${config.getString("ml.vector.host")}:${config.getString("ml.vector.port")}/ml/vector/ContentText"
  val mlKeywordAPIUrl = s"http://${config.getString("ml.keyword.host")}:${config.getString("ml.keyword.port")}/daggit/submit"
  val keywordAPIJobname = "vidyadaan_content_keyword_tagging"

  // Cassandra Configurations
  val dbTable: String = config.getString("lms-cassandra.table")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val csFieldMap = Map[String, String]("level1Concept" -> "level1_concept", "level2Concept" -> "level2_concept",
    "level3Concept" -> "level3_concept", "textbook_name" -> "textbook_name", "level1Name" -> "level1_name",
    "level2Name" -> "level2_name", "level3Name" -> "level3_name")

  val timeZone = if (config.hasPath("timezone")) config.getString("timezone") else "IST"
}
