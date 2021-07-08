package org.sunbird.job.qrimagegenerator.util

import java.awt._
import java.awt.font.TextAttribute
import java.awt.image.BufferedImage
import java.io.{File, IOException, InputStream}
import java.util
import java.util.UUID

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.zxing.client.j2se.BufferedImageLuminanceSource
import com.google.zxing.common.{BitMatrix, HybridBinarizer}
import com.google.zxing.qrcode.QRCodeWriter
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel
import com.google.zxing.{BarcodeFormat, EncodeHintType, NotFoundException, WriterException}
import javax.imageio.ImageIO
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.qrimagegenerator.functions.QRCodeImageGenerator
import org.sunbird.job.qrimagegenerator.task.QRCodeImageGeneratorConfig
import org.sunbird.job.util._

class QRCodeImageGeneratorUtil(config: QRCodeImageGeneratorConfig, cassandraUtil: CassandraUtil, cloudStorageUtil: org.sunbird.job.util.CloudStorageUtil) {
  private val qrCodeWriter = new QRCodeWriter()
  private val fontStore: util.HashMap[String, Font] = new util.HashMap[String, Font]()
  private val LOGGER = LoggerFactory.getLogger(classOf[QRCodeImageGeneratorUtil])

  @throws[WriterException]
  @throws[IOException]
  @throws[NotFoundException]
  @throws[FontFormatException]
  def createQRImages(qrGenRequest: QRCodeImageGenerator, appConfig: QRCodeImageGeneratorConfig, container: String, path: String, metrics: Metrics): util.List[File] = {
    val fileList = new util.ArrayList[File]
    val dataList = qrGenRequest.data
    val textList = qrGenRequest.text
    val fileNameList = qrGenRequest.fileName
    val errorCorrectionLevel = qrGenRequest.errorCorrectionLevel
    val pixelsPerBlock = qrGenRequest.pixelsPerBlock
    val qrMargin = qrGenRequest.qrCodeMargin
    val fontName = qrGenRequest.textFontName
    val fontSize = qrGenRequest.textFontSize
    val tracking = qrGenRequest.textCharacterSpacing
    val imageFormat = qrGenRequest.fileFormat
    val colorModel = qrGenRequest.colorModel
    val borderSize = qrGenRequest.imageBorderSize
    val qrMarginBottom = qrGenRequest.qrCodeMarginBottom
    val imageMargin = qrGenRequest.imageMargin
    val tempFilePath = qrGenRequest.tempFilePath

    for(i <- 0 to (dataList.size()-1)) {
      val data = dataList.get(i)
      val text = textList.get(i)
      val fileName = fileNameList.get(i)
      var qrImage = generateBaseImage(data, errorCorrectionLevel, pixelsPerBlock, qrMargin, colorModel)
      if (null != text || !(text.isEmpty)) {
        val textImage = getTextImage(text, fontName, fontSize, tracking, colorModel)
        qrImage = addTextToBaseImage(qrImage, textImage, colorModel, qrMargin, pixelsPerBlock, qrMarginBottom, imageMargin)
      }
      if (borderSize > 0) drawBorder(qrImage, borderSize, imageMargin)
      val finalImageFile = new File(tempFilePath + File.separator + fileName + "." + imageFormat)
      LOGGER.info("QRCodeImageGeneratorUtil:createQRImages: creating file - " + finalImageFile.getAbsolutePath)
      finalImageFile.createNewFile
      LOGGER.info("QRCodeImageGeneratorUtil:createQRImages: created file - " + finalImageFile.getAbsolutePath)
      println("QRCodeImageGeneratorUtil:createQRImages: created file - " + finalImageFile.getAbsolutePath)
      ImageIO.write(qrImage, imageFormat, finalImageFile)
      fileList.add(finalImageFile)
      try {
        val imageDownloadUrl = cloudStorageUtil.uploadFile(container, path, finalImageFile, false)
        updateCassandra(config.cassandraDialCodeImageTable, 2, imageDownloadUrl, "filename", fileName, metrics)
      } catch {
        case e: Exception =>
          metrics.incCounter(config.dbFailureEventCount)
          LOGGER.info("QRCodeImageGeneratorUtil: Failure while uploading download URL Query:: " + e.getMessage)
      }
    }
    fileList
  }

  def updateCassandra(table: String, status: Int, downloadURL: String, whereClauseKey: String, whereClauseValue: String, metrics: Metrics): Unit = {
    val value = if (whereClauseKey.equalsIgnoreCase("processid")) UUID.fromString(whereClauseValue) else whereClauseValue
    val updateQuery: String  = QueryBuilder.update(config.cassandraKeyspace, table)
      .`with`(QueryBuilder.set("status", status))
      .and(QueryBuilder.set("url", downloadURL))
      .where(QueryBuilder.eq(whereClauseKey, value)).toString
    cassandraUtil.upsert(updateQuery)
    metrics.incCounter(config.dbHitEventCount)
  }

  @throws[NotFoundException]
  private def addTextToBaseImage(qrImage: BufferedImage, textImage: BufferedImage, colorModel: String, qrMargin: Int, pixelsPerBlock: Int, qrMarginBottom: Int, imageMargin: Int) = {
    val qrSource = new BufferedImageLuminanceSource(qrImage)
    val qrBinarizer = new HybridBinarizer(qrSource)
    var qrBits = qrBinarizer.getBlackMatrix
    val textSource = new BufferedImageLuminanceSource(textImage)
    val textBinarizer = new HybridBinarizer(textSource)
    var textBits = textBinarizer.getBlackMatrix
    if (qrBits.getWidth > textBits.getWidth) {
      val tempTextMatrix = new BitMatrix(qrBits.getWidth, textBits.getHeight)
      copyMatrixDataToBiggerMatrix(textBits, tempTextMatrix)
      textBits = tempTextMatrix
    }
    else if (qrBits.getWidth < textBits.getWidth) {
      val tempQrMatrix = new BitMatrix(textBits.getWidth, qrBits.getHeight)
      copyMatrixDataToBiggerMatrix(qrBits, tempQrMatrix)
      qrBits = tempQrMatrix
    }
    val mergedMatrix = mergeMatricesOfSameWidth(qrBits, textBits, qrMargin, pixelsPerBlock, qrMarginBottom, imageMargin)
    getImage(mergedMatrix, colorModel)
  }

  @throws[WriterException]
  private def generateBaseImage(data: String, errorCorrectionLevel: String, pixelsPerBlock: Int, qrMargin: Int, colorModel: String) = {
    val hintsMap = getHintsMap(errorCorrectionLevel, qrMargin)
    val defaultBitMatrix = getDefaultBitMatrix(data, hintsMap)
    println("defaultBitMatrix: " + defaultBitMatrix.getWidth + " " + defaultBitMatrix.getHeight)
    val largeBitMatrix = getBitMatrix(data, defaultBitMatrix.getWidth * pixelsPerBlock, defaultBitMatrix.getHeight * pixelsPerBlock, hintsMap)
    println("largeBitMatrix width: " + largeBitMatrix.getWidth)
    println("largeBitMatrix width: " + largeBitMatrix.getHeight)
    getImage(largeBitMatrix, colorModel)
  }

  //To remove extra spaces between text and qrcode, margin below qrcode is removed
  //Parameter, qrCodeMarginBottom, is introduced to add custom margin(in pixels) between qrcode and text
  //Parameter, imageMargin is introduced, to add custom margin(in pixels) outside the black border of the image
  private def mergeMatricesOfSameWidth(firstMatrix: BitMatrix, secondMatrix: BitMatrix, qrMargin: Int, pixelsPerBlock: Int, qrMarginBottom: Int, imageMargin: Int) = {
    val mergedWidth = firstMatrix.getWidth + (2 * imageMargin)
    val mergedHeight = firstMatrix.getHeight + secondMatrix.getHeight + (2 * imageMargin)
    val defaultBottomMargin = pixelsPerBlock * qrMargin
    val marginToBeRemoved = if (qrMarginBottom > defaultBottomMargin) 0
    else defaultBottomMargin - qrMarginBottom
    val mergedMatrix = new BitMatrix(mergedWidth, mergedHeight - marginToBeRemoved)
    for(x <- 0 to (firstMatrix.getWidth-1)) {
      for(y <- 0 to (firstMatrix.getHeight-1)) {
        if (firstMatrix.get(x, y)) mergedMatrix.set(x + imageMargin, y + imageMargin)
      }
    }

    for(x <- 0 to (secondMatrix.getWidth-1)) {
      for(y <- 0 to (secondMatrix.getHeight-1)) {
        if (secondMatrix.get(x, y)) mergedMatrix.set(x + imageMargin, y + firstMatrix.getHeight - marginToBeRemoved + imageMargin)
      }
    }
    mergedMatrix
  }

  private def copyMatrixDataToBiggerMatrix(fromMatrix: BitMatrix, toMatrix: BitMatrix): Unit = {
    val widthDiff = toMatrix.getWidth - fromMatrix.getWidth
    val leftMargin = widthDiff / 2

    for(x <- 0 to (fromMatrix.getWidth-1)) {
      for(y <- 0 to (fromMatrix.getHeight-1)) {
        if (fromMatrix.get(x, y)) toMatrix.set(x + leftMargin, y)
      }
    }
  }

  private def drawBorder(image: BufferedImage, borderSize: Int, imageMargin: Int): Unit = {
    image.createGraphics
    val graphics = image.getGraphics.asInstanceOf[Graphics2D]
    graphics.setColor(Color.BLACK)
    for(i <- 0 to (borderSize-1)) {
      graphics.drawRect(i + imageMargin, i + imageMargin, image.getWidth - 1 - (2 * i) - (2 * imageMargin), image.getHeight - 1 - (2 * i) - (2 * imageMargin))
    }
    graphics.dispose()
  }

  private def getImage(bitMatrix: BitMatrix, colorModel: String) = {
    val imageWidth = bitMatrix.getWidth()
    val imageHeight = bitMatrix.getHeight()
    val image = new BufferedImage(imageWidth, imageHeight, getImageType(colorModel))
    image.createGraphics()
    val graphics = image.getGraphics.asInstanceOf[Graphics2D]
    graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF)
    graphics.setColor(Color.WHITE)
    graphics.fillRect(0, 0, imageWidth, imageHeight)
    graphics.setColor(Color.BLACK)
    println("imageWidth: " + imageWidth)
    println("imageWidth: " + imageHeight)
    for(i <- 0 to (imageWidth-1)) {
      for(j <- 0 to (imageHeight-1)) {
        if (bitMatrix.get(i, j)) {
          graphics.fillRect(i, j, 1, 1)
        }
      }
    }
    graphics.dispose()
    image
  }

  @throws[WriterException]
  private def getBitMatrix(data: String, width: Int, height: Int, hintsMap: util.Map[_, _]) = {
    qrCodeWriter.encode(data, BarcodeFormat.QR_CODE, width, height, hintsMap.asInstanceOf[util.Map[EncodeHintType, _]])
  }

  @throws[WriterException]
  private def getDefaultBitMatrix(data: String, hintsMap: util.Map[_, _]) = {
    qrCodeWriter.encode(data, BarcodeFormat.QR_CODE, 0, 0, hintsMap.asInstanceOf[util.Map[EncodeHintType, _]])
  }

  private def getHintsMap(errorCorrectionLevel: String, qrMargin: Int) = {
    val hintsMap = new util.HashMap[EncodeHintType, AnyRef]()
    errorCorrectionLevel match {
      case "H" => hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.H)
      case "Q" => hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.Q)
      case "M" => hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.M)
      case "L" => hintsMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.L)
    }
    hintsMap.put(EncodeHintType.MARGIN, qrMargin.asInstanceOf[AnyRef])
    hintsMap
  }

  //Sample = 2A42UH , Verdana, 11, 0.1, Grayscale
  @throws[IOException]
  @throws[FontFormatException]
  private def getTextImage(text: String, fontName: String, fontSize: Int, tracking: Double, colorModel: String) = {
    var image = new BufferedImage(1, 1, getImageType(colorModel))
    val basicFont = getFontFromStore(fontName)
    val attributes: java.util.Map[TextAttribute, Any] = new java.util.HashMap[TextAttribute, Any]
    attributes.put(TextAttribute.TRACKING, tracking)
    attributes.put(TextAttribute.WEIGHT, TextAttribute.WEIGHT_BOLD)
    attributes.put(TextAttribute.SIZE, fontSize)
    println("attributes: " + attributes)
    val font = basicFont.deriveFont(attributes)
    var graphics2d = image.createGraphics
    graphics2d.setFont(font)
    var fontmetrics = graphics2d.getFontMetrics
    val width = fontmetrics.stringWidth(text)
    val height = fontmetrics.getHeight
    graphics2d.dispose()
    image = new BufferedImage(width, height, getImageType(colorModel))
    graphics2d = image.createGraphics
    graphics2d.setRenderingHint(RenderingHints.KEY_ALPHA_INTERPOLATION, RenderingHints.VALUE_ALPHA_INTERPOLATION_QUALITY)
    graphics2d.setRenderingHint(RenderingHints.KEY_COLOR_RENDERING, RenderingHints.VALUE_COLOR_RENDER_QUALITY)
    graphics2d.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_OFF)
    graphics2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF)
    graphics2d.setColor(Color.WHITE)
    graphics2d.fillRect(0, 0, image.getWidth, image.getHeight)
    graphics2d.setColor(Color.BLACK)
    graphics2d.setFont(font)
    fontmetrics = graphics2d.getFontMetrics
    graphics2d.drawString(text, 0, fontmetrics.getAscent)
    graphics2d.dispose()
    image
  }

  private def getImageType(colorModel: String) = if (colorModel.equalsIgnoreCase("RGB")) BufferedImage.TYPE_INT_RGB
  else BufferedImage.TYPE_BYTE_GRAY

  @throws[IOException]
  @throws[FontFormatException]
  //load the packaged font file from the root dir
  private def loadFontStore(fontName: String) = {
    val fontFile = fontName + ".ttf"
    println("fontFile: " + fontFile)
    var basicFont: Font = null
    var fontStream: InputStream = null
    val classLoader: ClassLoader = this.getClass.getClassLoader
    try {
      fontStream = classLoader.getResourceAsStream(fontFile)
      println("input stream value is not null for fontfile " + fontFile + " " + fontStream)
      LOGGER.info("input stream value is not null for fontfile " + fontFile + " " + fontStream)
      basicFont = Font.createFont(Font.TRUETYPE_FONT, fontStream)
    } catch {
      case e: Exception =>
        println("Exception occurred during font creation " + e.getMessage)
        e.printStackTrace()
        LOGGER.debug("Exception occurred during font creation " + e.getMessage)
    }
    fontStore.put(fontName, basicFont)
    basicFont
  }

  @throws[IOException]
  @throws[FontFormatException]
  private def getFontFromStore(fontName: String): Font = {
    fontStore.getOrDefault(fontName, loadFontStore(fontName))
  }
}

