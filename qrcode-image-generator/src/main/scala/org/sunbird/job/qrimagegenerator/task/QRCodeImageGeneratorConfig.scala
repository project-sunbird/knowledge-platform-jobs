package org.sunbird.job.qrimagegenerator.task

import com.typesafe.config.Config
import org.apache.commons.lang3.StringUtils
import org.sunbird.job.BaseJobConfig

class QRCodeImageGeneratorConfig(override val config: Config) extends BaseJobConfig(config, "qrcode-image-generator") {

  private val serialVersionUID = 2905979434303791378L

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val dbHitEventCount = "db-hit-events-count"
  val dbFailureEventCount = "db-failure-events-count"
  val skippedEventCount = "skipped-events-count"
  val cloudDbHitCount = "cloud-db-hit-events-count"
  val cloudDbFailCount = "cloud-db-hit-failure-count"

  // Consumers
  val eventConsumer = "qrcode-image-generator-consumer"
  val qrCodeImageGeneratorFunction = "qrcode-image-generator-function"

  val configVersion = "1.0"

  val eid = "BE_QR_IMAGE_GENERATOR"
  val lpTempfileLocation = config.getString("lp.tmp.file.location")

  //Cloud store details
  val cloudStorageType= config.getString("cloud.storage.type")
  val cloudUploadRetryCount= config.getInt("cloud.upload.retry.count")
  val azureStorageKey= config.getString("azure.storage.key")
  val azureStorageSecret= config.getString("azure.storage.secret")
  val awsStorageKey= config.getString("aws.storage.key")
  val awsStorageSecret= config.getString("aws.storage.secret")
  val storageKey = if (StringUtils.equalsIgnoreCase(cloudStorageType, "azure")) azureStorageKey else awsStorageKey
  val storageSecret = if (StringUtils.equalsIgnoreCase(cloudStorageType, "azure")) azureStorageSecret else awsStorageSecret

  //image Margings
  val qrImageBottomMargin= config.getInt("qr.image.bottom.margin")
  val qrImageMargin= config.getInt("qr.image.margin")

  //cassandra config
  val cassandraHost= config.getString("lms-cassandra.host")
  val cassandraPort= config.getInt("lms-cassandra.port")
  val cassandraKeyspace = config.getString("lms-cassandra.keyspace")
  val cassandraDialCodeImageTable = config.getString("lms-cassandra.dialcode.imageTable")
  val cassandraDialCodeBatchTable = config.getString("lms-cassandra.dialcode.batchTable")

  def getString(key: String, default: String): String = {
    if (config.hasPath(key)) config.getString(key) else default
  }
}
