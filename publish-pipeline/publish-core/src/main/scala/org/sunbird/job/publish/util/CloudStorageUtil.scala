package org.sunbird.job.publish.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.Slug

import java.io.File

class CloudStorageUtil(config: PublishConfig) extends Serializable {

	var storageService: BaseStorageService = null
	val cloudStorageType = config.getString("cloud_storage_type", "azure")
	val azureStorageContainer = config.getString("azure_storage_container", "")
	val awsStorageContainer = config.getString("aws_storage_container", "")

	@throws[Exception]
	def getService: BaseStorageService = {
		if (null == storageService) {
			if (StringUtils.equalsIgnoreCase(cloudStorageType, "azure")) {
				val azureStorageKey = config.getString("azure_storage_key", "")
				val azureStorageSecret = config.getString("azure_storage_secret", "")
				storageService = StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, azureStorageKey, azureStorageSecret))
			} else if (StringUtils.equalsIgnoreCase(cloudStorageType, "aws")) {
				val awsStorageKey = config.getString("aws_storage_key", "")
				val awsStorageSecret = config.getString("aws_storage_secret", "")
				storageService = StorageServiceFactory.getStorageService(StorageConfig(cloudStorageType, awsStorageKey, awsStorageSecret))
			} else throw new Exception("Error while initialising cloud storage: " + cloudStorageType)
		}
		storageService
	}

	def getContainerName(): String = {
		cloudStorageType match {
			case "azure" => azureStorageContainer
			case "aws" => awsStorageContainer
			case _ => throw new Exception("Container name not configured.")
		}
	}

	def uploadFile(folderName: String, file: File, slug: Option[Boolean] = Option(true)): Array[String] = {
		val slugFile = if (slug.getOrElse(true)) Slug.createSlugFile(file) else file
		val objectKey = folderName + "/" + slugFile.getName
		val url = getService.upload(getContainerName, slugFile.getAbsolutePath, objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty)
		Array[String](objectKey, url)
	}

	def copyObjectsByPrefix(source: String, destination: String) = {
		getService.copyObjects(getContainerName, source, getContainerName, destination, Option.apply(true))
	}

	def getURI(prefix: String, isDirectory: Option[Boolean]): String = {
		getService.getUri(getContainerName, prefix, isDirectory)
	}

	def uploadDirectory(folderName: String, directory: File, slug: Option[Boolean] = Option(true)): Array[String] = {
		val slugFile = if (slug.getOrElse(true)) Slug.createSlugFile(directory) else directory
		val objectKey = folderName + File.separator
		val url = getService.upload(getContainerName(), slugFile.getAbsolutePath, objectKey, Option.apply(true), Option.apply(1), Option.apply(5), Option.empty)
		Array[String](objectKey, url)
	}

	def deleteFile(key: String, isDirectory: Option[Boolean] = Option(false)) {
		getService.deleteObject(getContainerName, key, isDirectory)
	}

}
