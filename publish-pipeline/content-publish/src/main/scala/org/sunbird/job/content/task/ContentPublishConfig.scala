package org.sunbird.job.content.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.content.publish.domain.PublishMetadata

import java.util
import scala.collection.JavaConverters._

class ContentPublishConfig(override val config: Config) extends PublishConfig(config, "content-publish") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val publishMetaTypeInfo: TypeInformation[PublishMetadata] = TypeExtractor.getForClass(classOf[PublishMetadata])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val postPublishTopic: String = config.getString("kafka.post_publish.topic")
  val inputConsumerName = "content-publish-consumer"

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val eventRouterParallelism: Int = config.getInt("task.router.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val skippedEventCount = "skipped-event-count"
  val contentPublishEventCount = "content-publish-count"
  val contentPublishSuccessEventCount = "content-publish-success-count"
  val contentPublishFailedEventCount = "content-publish-failed-count"
  val videoStreamingGeneratorEventCount = "video-streaming-event-count"
  //	val collectionPublishEventCount = "collection-publish-count"
  //	val collectionPublishSuccessEventCount = "collection-publish-success-count"
  //	val collectionPublishFailedEventCount = "collection-publish-failed-count"

  // Cassandra Configurations
  val cassandraHost: String = config.getString("lms-cassandra.host")
  val cassandraPort: Int = config.getInt("lms-cassandra.port")
  // TODO: Need to check respective changes for content
  val contentKeyspaceName = config.getString("content.keyspace")
  val contentTableName = config.getString("content.table")

  // Neo4J Configurations
  val graphRoutePath = config.getString("neo4j.routePath")
  val graphName = config.getString("neo4j.graph")

  // Out Tags
  val contentPublishOutTag: OutputTag[PublishMetadata] = OutputTag[PublishMetadata]("content-publish")
  val collectionPublishOutTag: OutputTag[PublishMetadata] = OutputTag[PublishMetadata]("collection-publish")
  val generateVideoStreamingOutTag: OutputTag[String] = OutputTag[String]("video-streaming-generator-request")

  // Service Urls
  val printServiceBaseUrl: String = config.getString("service.print.basePath")

  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val schemaSupportVersionMap = if (config.hasPath("schema.supportedVersion")) config.getObject("schema.supportedVersion").unwrapped().asScala.toMap else Map[String, AnyRef]()

  val supportedObjectType: util.List[String] = if (config.hasPath("content.objectType")) config.getStringList("content.objectType") else util.Arrays.asList[String]("Content", "ContentImage")
  val supportedMimeType: util.List[String] = if (config.hasPath("content.mimeType")) config.getStringList("content.mimeType") else util.Arrays.asList[String]("application/pdf")
  val streamableMimeType: util.List[String] = if (config.hasPath("content.stream.mimeType")) config.getStringList("content.stream.mimeType") else util.Arrays.asList[String]("video/mp4")
  val isStreamingEnabled: Boolean = if (config.hasPath("content.stream.enabled")) config.getBoolean("content.stream.enabled") else false

  val contentFolder: String = if(config.hasPath("cloud_storage.content.folder")) config.getString("cloud_storage.content.folder") else "content"
  val artifactFolder: String = if(config.hasPath("cloud_storage.artifact.folder")) config.getString("cloud_storage.artifact.folder") else "artifact"
  val retryAssetDownloadsCount: Integer = if(config.hasPath("content.retry_asset_download_count")) config.getInt("content.retry_asset_download_count") else 1
  val pluginMediaBaseUrl = if(config.hasPath("plugin.media.base.url")) config.getString("plugin.media.base.url") else ""
  val contentMediaBaseUrl = if(config.hasPath("content.media.base.url")) config.getString("plugin.media.base.url") else ""
  val isECARExtractionEnabled = if(config.hasPath("content.isECARExtractionEnabled")) config.getBoolean("content.isECARExtractionEnabled") else true
}
