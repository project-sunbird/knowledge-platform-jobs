package org.sunbird.job.content.publish.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.content.task.ContentPublishConfig
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData, Slug}
import org.sunbird.job.publish.helpers._
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.zip.ZipFile
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._

trait ContentPublisher extends ObjectReader with ObjectValidator with ObjectEnrichment with EcarGenerator with ObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[ContentPublisher])
  private val level4MimeTypes = List("video/x-youtube", "application/pdf", "application/msword", "application/epub", "application/vnd.ekstep.h5p-archive", "text/x-url")
  private val level4ContentTypes = List("Course", "CourseUnit", "LessonPlan", "LessonPlanUnit")
  private val pragmaMimeTypes = List("video/x-youtube", "application/pdf")
  private val extractableMimeTypes = List("application/vnd.ekstep.ecml-archive", "application/vnd.ekstep.html-archive", "application/vnd.ekstep.plugin-archive", "application/vnd.ekstep.h5p-archive")
  private val tempFileLocation = "/data/contentBundle/"
  protected val extractablePackageExtensions = List(".zip", ".h5p", ".epub")

  override def getExtData(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[ObjectExtData] = None

  override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

  override def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig): Option[ObjectData] = {
    val updatedMeta: Map[String, AnyRef] = obj.metadata ++ Map("pkgVersion" -> (obj.pkgVersion + 1).asInstanceOf[AnyRef],
      "lastPublishedOn" -> getTimeStamp, "flagReasons" -> null, "body" -> null, "publishError" -> null,
      "variants" -> null, "downloadUrl" -> null)
    val updatedCompatibilityLevel = setCompatibilityLevel(obj, updatedMeta).getOrElse(updatedMeta)
    val updatedPragma = setPragma(obj, updatedCompatibilityLevel).getOrElse(updatedCompatibilityLevel)

    //delete basePath if exists
    Files.deleteIfExists(new File(getBasePath(obj.identifier)).toPath)

    val contentConfig =  config.asInstanceOf[ContentPublishConfig]
    if(contentConfig.isECARExtractionEnabled && extractableMimeTypes.contains(obj.mimeType)){
      copyExtractedContentPackage(obj, contentConfig, "version", cloudStorageUtil)
      copyExtractedContentPackage(obj, contentConfig, "latest", cloudStorageUtil)
    }
    val updatedPreviewUrl = updatePreviewUrl(obj, updatedPragma, cloudStorageUtil, contentConfig).getOrElse(updatedPragma)
    Some(new ObjectData(obj.identifier, updatedPreviewUrl, obj.extData, obj.hierarchy))
  }

  override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = {
    Some(List(obj.metadata ++ obj.extData.getOrElse(Map()).filter(p => !excludeBundleMeta.contains(p._1.asInstanceOf[String]))))
  }

  override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = None

  def validateMetadata(obj: ObjectData, identifier: String): List[String] = {
    logger.info("Validating Content metadata for : " + obj.identifier)
    val messages = ListBuffer[String]()
    if (obj.mimeType.equalsIgnoreCase("application/vnd.ekstep.ecml-archive") && (obj.extData.getOrElse("body", "") == null || obj.extData.getOrElse("body", "").asInstanceOf[Map[String, AnyRef]].isEmpty))
      messages += s"""There is no body available for : $identifier"""
    else if(StringUtils.isBlank(obj.metadata.getOrElse("artifactUrl", "").asInstanceOf[String]))
      messages += s"""There is no artifactUrl available for : $identifier"""

    messages.toList
  }

  def getObjectWithEcar(data: ObjectData, pkgTypes: List[String])(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil, defCache: DefinitionCache, defConfig: DefinitionConfig, httpUtil: HttpUtil): ObjectData = {
    logger.info("ContentPublisher:getObjectWithEcar: Ecar generation done for Content: " + data.identifier)
    val ecarMap: Map[String, String] = generateEcar(data, pkgTypes)
    val variants: java.util.Map[String, java.util.Map[String, String]] = ecarMap.map { case (key, value) => key.toLowerCase -> Map[String, String]("ecarUrl"-> value, "size"-> httpUtil.getSize(value).toString).asJava }.asJava
    logger.info("ContentPublisher ::: getObjectWithEcar ::: ecar map ::: " + ecarMap)
    val meta: Map[String, AnyRef] = Map("downloadUrl" -> ecarMap.getOrElse(EcarPackageType.FULL.toString, ""), "variants" -> variants)
    new ObjectData(data.identifier, data.metadata ++ meta, data.extData, data.hierarchy)
  }

  private def setCompatibilityLevel(obj: ObjectData, updatedMeta: Map[String, AnyRef]): Option[Map[String, AnyRef]] = {
    if (level4MimeTypes.contains(obj.mimeType)
      || level4ContentTypes.contains(obj.getString("contentType", ""))) {
      logger.info("setting compatibility level for content id : " + obj.identifier + " as 4.")
      Some(updatedMeta ++ Map("compatibilityLevel" -> 4.asInstanceOf[AnyRef]))
    } else None
  }

  private def setPragma(obj: ObjectData, updatedMeta: Map[String, AnyRef]): Option[Map[String, AnyRef]] = {
    if (pragmaMimeTypes.contains(obj.mimeType)) {
      val pgm: java.util.List[String] = obj.metadata.getOrElse("pragma", new java.util.ArrayList[String]()).asInstanceOf[java.util.List[String]]
      val pragma:List[String] = pgm.asScala.toList
      val value = "external"
      if(!pragma.contains(value)) {
        Some(updatedMeta ++ Map("pragma" -> (pragma ++ List(value))))
      } else None
    } else None
  }

  private def updatePreviewUrl(obj: ObjectData, updatedMeta: Map[String, AnyRef], cloudStorageUtil: CloudStorageUtil, config: ContentPublishConfig): Option[Map[String, AnyRef]] = {
    if (StringUtils.isNotBlank(obj.mimeType)) {
      logger.debug("Checking Required Fields For: " + obj.mimeType)
      obj.mimeType match {
        case "application/vnd.ekstep.content-collection" | "application/vnd.ekstep.plugin-archive"
             | "application/vnd.android.package-archive" | "assets" =>
        None
        case "application/vnd.ekstep.ecml-archive" | "application/vnd.ekstep.html-archive" | "application/vnd.ekstep.h5p-archive" =>
          val latestFolderS3Url = getS3URL(obj, cloudStorageUtil, config)
          val updatedPreviewUrl = updatedMeta ++ Map("previewUrl" -> latestFolderS3Url, "streamingUrl" -> latestFolderS3Url)
          Some(updatedPreviewUrl)
        case _ =>
          val artifactUrl = obj.getString("artifactUrl", null)
          val updatedPreviewUrl = updatedMeta ++ Map("previewUrl" -> artifactUrl)
          if (config.streamableMimeType.contains(obj.mimeType)) Some(updatedPreviewUrl ++ Map("streamingUrl"-> artifactUrl)) else Some(updatedPreviewUrl)
      }
    } else None
  }


  def getS3URL(obj: ObjectData, cloudStorageUtil: CloudStorageUtil, config: ContentPublishConfig): String = {
    val path = getExtractionPath(obj, config, "latest")
    var isDirectory = false
    if (extractableMimeTypes.contains(obj.mimeType)) isDirectory = true
    cloudStorageUtil.getURI(path, Option.apply(isDirectory))
  }

  private def getExtractionPath(obj: ObjectData, config: ContentPublishConfig, prefix: String):String = {
    obj.mimeType match {
      case "application/vnd.ekstep.ecml-archive" => config.contentFolder + File.separator + "ecml" + File.separator + obj.identifier + "-" + prefix
      case "application/vnd.ekstep.html-archive" => config.contentFolder + File.separator + "html" + File.separator + obj.identifier + "-" + prefix
      case "application/vnd.ekstep.h5p-archive" => config.contentFolder + File.separator + "h5p" + File.separator + obj.identifier + "-" + prefix
      case _ => ""
    }
  }

  def getBasePath(objectId: String): String = {
    if (!StringUtils.isBlank(objectId)) tempFileLocation + File.separator + System.currentTimeMillis + "_temp" + File.separator + objectId else ""
  }

  def copyExtractedContentPackage(obj: ObjectData, contentConfig: ContentPublishConfig, extractionType: String, cloudStorageUtil: CloudStorageUtil): Unit = {
    if (!isExtractedSnapshotExist(obj)) throw new Exception("Error! Snapshot Type Extraction doesn't Exists.")
    val sourcePrefix = getExtractionPath(obj, contentConfig, "snapshot")
    val destinationPrefix = getExtractionPath(obj, contentConfig, extractionType)
    cloudStorageUtil.copyObjectsByPrefix(sourcePrefix, destinationPrefix)
  }

  private def isExtractedSnapshotExist(obj: ObjectData): Boolean = {
    val artifactUrl = obj.getString("artifactUrl", null)
    isValidSnapshotFile(artifactUrl)
  }

  private def isValidSnapshotFile(artifactUrl: String): Boolean = {
    var isValid = false
    if (StringUtils.isNotBlank(artifactUrl))
      for (key <- extractablePackageExtensions)
        if (StringUtils.endsWithIgnoreCase(artifactUrl, key)) isValid = true
    isValid
  }

}
