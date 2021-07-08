package org.sunbird.job.assetenricment.helpers

import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.bytedeco.javacv.{FFmpegFrameGrabber, Java2DFrameConverter}
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.assetenricment.domain.Event
import org.sunbird.job.assetenricment.models.Asset
import org.sunbird.job.assetenricment.task.AssetEnrichmentConfig
import org.sunbird.job.assetenricment.util._
import org.sunbird.job.assetenricment.util.CloudStorageUtil
import org.sunbird.job.util._

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.imageio.ImageIO
import scala.collection.mutable

trait VideoEnrichmentHelper extends ThumbnailUtil {

  private[this] val logger = LoggerFactory.getLogger(classOf[VideoEnrichmentHelper])
  private val CONTENT_FOLDER = "content"
  private val ARTIFACT_FOLDER = "artifact"

  def enrichVideo(asset: Asset)(config: AssetEnrichmentConfig, youTubeUtil: YouTubeUtil, cloudStorageUtil: CloudStorageUtil, neo4JUtil: Neo4JUtil): Asset = {
    try {
      val videoUrl = asset.artifactUrl
      enrichVideo(asset, videoUrl)(youTubeUtil, cloudStorageUtil, neo4JUtil, config)
    } catch {
      case e: Exception =>
        logger.error(s"Something Went Wrong While Performing Asset Enrichment operation. Content Id: ${asset.identifier}. ", e)
        asset.put("processingError", e.getMessage)
        asset.put("status", "Failed")
        neo4JUtil.updateNode(asset.identifier, asset.getMetadata)
        throw e
    }
  }

  private def enrichVideo(asset: Asset, videoUrl: String)(youTubeUtil: YouTubeUtil, cloudStorageUtil: CloudStorageUtil, neo4JUtil: Neo4JUtil, config: AssetEnrichmentConfig): Asset = {
    if (StringUtils.equalsIgnoreCase("video/x-youtube", asset.mimeType)) processYoutubeVideo(asset, videoUrl)(youTubeUtil)
    else processOtherVideo(asset, videoUrl)(cloudStorageUtil, config)
    asset.put("status", "Live")
    neo4JUtil.updateNode(asset.identifier, asset.getMetadata)
    asset
  }

  def processYoutubeVideo(asset: Asset, videoUrl: String)(youTubeUtil: YouTubeUtil): Unit = {
    val data = youTubeUtil.getVideoInfo(videoUrl, "snippet,contentDetails", List[String]("thumbnail", "duration"))
    if (data.nonEmpty) {
      if (data.contains("thumbnail")) asset.put("thumbnail", data("thumbnail").asInstanceOf[String])
      if (data.contains("duration")) asset.put("duration", data("duration").toString)
    } else throw new Exception(s"Failed to get data for Youtube Video Url : ${videoUrl} and identifier: ${asset.identifier}.")
  }

  def processOtherVideo(asset: Asset, videoUrl: String)(implicit cloudStorageUtil: CloudStorageUtil, config: AssetEnrichmentConfig): Unit = {
    val videoFile = AssetFileUtils.copyURLToFile(asset.identifier, videoUrl, videoUrl.substring(videoUrl.lastIndexOf("/") + 1))
    try {
      videoFile match {
        case Some(file: File) => enrichVideo(asset, file)(cloudStorageUtil, config)
        case _ => logger.error("ERR_INVALID_FILE_URL", s"Please Provide Valid File Url for identifier: ${asset.identifier}!")
          throw new Exception(s"Invalid Artifact Url for identifier: ${asset.identifier}!")
      }
    } finally {
      AssetFileUtils.deleteDirectory(new File(s"/tmp/${asset.identifier}"))
    }
  }

  private def enrichVideo(asset: Asset, videoFile: File)(implicit cloudStorageUtil: CloudStorageUtil, config: AssetEnrichmentConfig): Unit = {
    val frameGrabber = new FFmpegFrameGrabber(videoFile)
    frameGrabber.start()
    val videoDuration = frameGrabber.getLengthInTime
    if (videoDuration != 0) asset.put("duration", TimeUnit.MICROSECONDS.toSeconds(videoDuration).toString)
    val numberOfFrames = frameGrabber.getLengthInFrames
    val thumbNail = fetchThumbNail(asset.identifier, config.sampleThumbnailCount, config.thumbnailSize, numberOfFrames, frameGrabber)
    frameGrabber.stop()
    if (null != thumbNail && thumbNail.exists) {
      logger.info("Thumbnail created for Content Id: " + asset.identifier)
      val urlArray = upload(thumbNail, asset.identifier)(cloudStorageUtil)
      val thumbUrl = urlArray(1)
      asset.put("thumbnail", thumbUrl)
    } else logger.error(s"Thumbnail could not be generated for identifier : ${asset.identifier}.")
  }

  private def fetchThumbNail(identifier: String, sampleThumbnailCount: Int, thumbnailSize: Int, numberOfFrames: Long, frameGrabber: FFmpegFrameGrabber): File = {
    val converter = new Java2DFrameConverter
    var thumbnail: File = null
    var colorCount = 0
    for (i <- 1 to sampleThumbnailCount) {
      val inFile = AssetFileUtils.createFile(s"${AssetFileUtils.getBasePath(identifier)}${File.separator}${System.currentTimeMillis}.png")
      frameGrabber.setFrameNumber((numberOfFrames / sampleThumbnailCount).toInt * i)
      try {
        val bufferedImage = converter.convert(frameGrabber.grabImage())
        if (null != bufferedImage) {
          ImageIO.write(bufferedImage, "png", inFile)
          val outFile = generateOutFile(inFile, thumbnailSize)
          outFile match {
            case Some(file: File) =>
              val tmpColorCount = getImageColor(file)
              if (colorCount < tmpColorCount) {
                colorCount = tmpColorCount
                thumbnail = file
              }
            case _ => logger.error(s"Thumbnail Could Not Be Generated For : ${identifier}.")
          }
        }
      } catch {
        case e: Throwable =>
          logger.error(s"Exception while generating thumbnail for identifier: ${identifier}.", e)
          throw new Exception(s"Exception while generating thumbnail for identifier: ${identifier}. Error : ${e.getMessage}", e)
      }
    }
    thumbnail
  }

  private def getImageColor(imagePath: File): Int = {
    val image = ImageIO.read(imagePath)
    val colorSet = mutable.Set[Int]()
    for (r <- 0 until image.getHeight) {
      for (c <- 0 until image.getWidth) {
        val colorCode = image.getRGB(c, r)
        colorSet.add(colorCode)
      }
    }
    colorSet.size
  }

  def upload(uploadedFile: File, identifier: String)(implicit cloudStorageUtil: CloudStorageUtil): Array[String] = {
    try {
      val slug = Slug.makeSlug(identifier, true)
      val folder = s"${CONTENT_FOLDER}/${slug}/${ARTIFACT_FOLDER}"
      cloudStorageUtil.uploadFile(folder, uploadedFile, Some(true))
    } catch {
      case e: Exception =>
        throw new Exception("Error while uploading File to cloud for identifier : ${identifier}.", e)
    }
  }

  def pushStreamingUrlEvent(asset: Asset, context: ProcessFunction[Event, String]#Context)(implicit metrics: Metrics, config: AssetEnrichmentConfig): Unit = {
    if (config.isStreamingEnabled && config.streamableMimeType.contains(asset.get("mimeType", "").asInstanceOf[String])) {
      val event = getStreamingEvent(asset)
      context.output(config.generateVideoStreamingOutTag, event)
      metrics.incCounter(config.videoStreamingGeneratorEventCount)
    }
  }

  def getStreamingEvent(asset: Asset)(implicit config: AssetEnrichmentConfig) : String = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.${ets}.${UUID.randomUUID}"""
    val metadata = asset.getMetadata
    val channelId = metadata.getOrElse("channel", "").asInstanceOf[String]
    val ver = metadata.getOrElse("versionKey", "").asInstanceOf[String]
    val event = s"""{"eid":"BE_JOB_REQUEST", "ets": ${ets}, "mid": "${mid}", "actor": {"id": "Post Publish Processor", "type": "System"}, "context":{"pdata":{"ver":"1.0","id":"org.ekstep.platform"}, "channel":"${channelId}","env":"${config.jobEnv}"},"object":{"ver":"${ver}","id":"${asset.identifier}"},"edata": {"action":"post-publish-process","iteration":1,"identifier":"${asset.identifier}","channel":"${channelId}","artifactUrl":"${asset.artifactUrl}","mimeType":"video/mp4","contentType":"Resource","pkgVersion":1,"status":"Live"}}""".stripMargin
    logger.info(s"Video Streaming Event for identifier ${asset.identifier}  is  : ${event}")
    event
  }

}
