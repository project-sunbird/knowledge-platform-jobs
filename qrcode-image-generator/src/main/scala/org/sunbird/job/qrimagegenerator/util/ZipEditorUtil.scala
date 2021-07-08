package org.sunbird.job.qrimagegenerator.util

import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.util
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.slf4j.LoggerFactory

object ZipEditorUtil {

  private val LOGGER = LoggerFactory.getLogger("ZipEditorUtil")

  @throws[IOException]
  def zipFiles(files: util.List[File], zipName: String, basePath: String): File = {
    val zipFile: File = new File(basePath + File.separator + zipName + ".zip")
    LOGGER.info("ZipEditorUtil:zipFiles: creating file - " + zipFile.getAbsolutePath())
    zipFile.createNewFile
    LOGGER.info("ZipEditorUtil:zipFiles: created file - " + zipFile.getAbsolutePath())
    val fos: FileOutputStream = new FileOutputStream(zipFile)
    val zos: ZipOutputStream = new ZipOutputStream(fos)
    import scala.collection.JavaConversions._
    for (file <- files) {
      val filePath: String = file.getAbsolutePath()
      val ze: ZipEntry = new ZipEntry(file.getName)
      zos.putNextEntry(ze)
      val fis: FileInputStream = new FileInputStream(filePath)
      val buffer: Array[Byte] = new Array[Byte](1024)
      var len: Int = Option(fis.read(buffer)).getOrElse(0)

      while (len  > 0) {
        zos.write(buffer, 0, len)
      }
      zos.closeEntry()
      fis.close()
    }
    zos.close()
    fos.close()
    zipFile
  }
}