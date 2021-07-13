package org.sunbird.job.questionset.publish.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.apache.commons.lang3
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers._
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

trait QuestionPublisher extends ObjectReader with ObjectValidator with ObjectEnrichment with EcarGenerator with ObjectUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionPublisher])
	val extProps = List("body", "editorState", "answer", "solutions", "instructions", "hints", "media", "responseDeclaration", "interactions")

	override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = None

	override def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig): Option[ObjectData] = {
		val pkgVersion = obj.metadata.getOrElse("pkgVersion", 0.0.asInstanceOf[Number]).asInstanceOf[Number].intValue() + 1
		val publishType = obj.metadata.getOrElse("publish_type", "Public").asInstanceOf[String]
		val status = if (StringUtils.equals("Private", publishType)) "Unlisted" else "Live"
		val updatedMeta = obj.metadata ++ Map("identifier"->obj.identifier, "pkgVersion" -> pkgVersion.asInstanceOf[AnyRef], "status"->status)
		Some(new ObjectData(obj.identifier, updatedMeta, obj.extData, obj.hierarchy))
	}

	def validateQuestion(obj: ObjectData, identifier: String): List[String] = {
		logger.info("Validating Question External Data For : " + obj.identifier)
		//TODO: Remove Below logger statement
		logger.info("Question External Data : " + obj.extData.get)
		val messages = ListBuffer[String]()
		if (obj.extData.getOrElse(Map()).getOrElse("body", "").asInstanceOf[String].isEmpty) messages += s"""There is no body available for : $identifier"""
		if (obj.extData.getOrElse(Map()).getOrElse("editorState", "").asInstanceOf[String].isEmpty) messages += s"""There is no editorState available for : $identifier"""
		val interactionTypes = obj.metadata.getOrElse("interactionTypes", new util.ArrayList[String]()).asInstanceOf[util.List[String]].asScala.toList
		interactionTypes.nonEmpty match {
			case true => {
				if (obj.extData.get.getOrElse("responseDeclaration", "").asInstanceOf[String].isEmpty) messages += s"""There is no responseDeclaration available for : $identifier"""
				if (obj.extData.get.getOrElse("interactions", "").asInstanceOf[String].isEmpty) messages += s"""There is no interactions available for : $identifier"""
			}
			case false => if (obj.extData.getOrElse(Map()).getOrElse("answer", "").asInstanceOf[String].isEmpty) messages += s"""There is no answer available for : $identifier"""
		}
		messages.toList
	}

	override def getExtData(identifier: String,pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[ObjectExtData] = {
		val row: Row = Option(getQuestionData(getEditableObjId(identifier, pkgVersion), readerConfig)).getOrElse(getQuestionData(identifier, readerConfig))
		//TODO: covert below code in scala. entire props should be in scala.
		val data = if (null != row) Option(extProps.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2.asInstanceOf[String]))) else Option(Map[String, AnyRef]())
		Option(ObjectExtData(data))
	}

	def getQuestionData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Row = {
		logger.info("QuestionPublisher ::: getQuestionData ::: Reading Question External Data For : "+identifier)
		val select = QueryBuilder.select()
		extProps.foreach(prop => if (lang3.StringUtils.equals("body", prop) | lang3.StringUtils.equals("answer", prop)) select.fcall("blobAsText", QueryBuilder.column(prop.toLowerCase())).as(prop.toLowerCase()) else select.column(prop.toLowerCase()).as(prop.toLowerCase()))
		val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where().and(QueryBuilder.eq("identifier", identifier))
		logger.info("Cassandra Fetch Query :: "+ selectWhere.toString)
		cassandraUtil.findOne(selectWhere.toString)
	}

	def dummyFunc = (obj: ObjectData, readerConfig: ExtDataConfig) => {}

	override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		None
	}

	override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		None
	}

	override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		val extData = obj.extData.getOrElse(Map())
		val identifier = obj.identifier.replace(".img", "")
		val query = QueryBuilder.update(readerConfig.keyspace, readerConfig.table)
			.where(QueryBuilder.eq("identifier", identifier))
			.`with`(QueryBuilder.set("body",  QueryBuilder.fcall("textAsBlob", extData.getOrElse("body", null))))
			.and(QueryBuilder.set("answer", QueryBuilder.fcall("textAsBlob", extData.getOrElse("answer", null))))
			.and(QueryBuilder.set("editorstate", extData.getOrElse("editorState", null)))
			.and(QueryBuilder.set("solutions", extData.getOrElse("solutions", null)))
			.and(QueryBuilder.set("instructions", extData.getOrElse("instructions", null)))
			.and(QueryBuilder.set("media", extData.getOrElse("media", null)))
			.and(QueryBuilder.set("hints", extData.getOrElse("hints", null)))
			.and(QueryBuilder.set("responsedeclaration", extData.getOrElse("responseDeclaration", null)))
			.and(QueryBuilder.set("interactions", extData.getOrElse("interactions", null)))

		logger.info(s"Updating Question in Cassandra For ${identifier} : ${query.toString}")
		val result = cassandraUtil.upsert(query.toString)
		if (result) {
			logger.info(s"Question Updated Successfully For ${identifier}")
		} else {
			val msg = s"Question Update Failed For ${identifier}"
			logger.error(msg)
			throw new Exception(msg)
		}
	}

	override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
		val query = QueryBuilder.delete().from(readerConfig.keyspace, readerConfig.table)
			.where(QueryBuilder.eq("identifier", obj.dbId))
			.ifExists
		cassandraUtil.session.execute(query.toString)
	}

	override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = {
		Some(List(obj.metadata ++ obj.extData.getOrElse(Map()).filter(p => !excludeBundleMeta.contains(p._1.asInstanceOf[String]))))
	}

	def getObjectWithEcar(data: ObjectData, pkgTypes: List[String])(implicit ec: ExecutionContext, cloudStorageUtil: CloudStorageUtil, defCache: DefinitionCache, defConfig: DefinitionConfig, httpUtil: HttpUtil): ObjectData = {
		logger.info("QuestionPublishFunction:generateECAR: Ecar generation done for Question: " + data.identifier)
		val ecarMap: Map[String, String] = generateEcar(data, pkgTypes)
		val variants: java.util.Map[String, java.util.Map[String, String]] = ecarMap.map { case (key, value) => key.toLowerCase -> Map[String, String]("ecarUrl"-> value, "size"-> httpUtil.getSize(value).toString).asJava }.asJava
		logger.info("QuestionSetPublishFunction ::: generateECAR ::: ecar map ::: " + ecarMap)
		val meta: Map[String, AnyRef] = Map("downloadUrl" -> ecarMap.getOrElse(EcarPackageType.FULL.toString, ""), "variants" -> variants)
		new ObjectData(data.identifier, data.metadata ++ meta, data.extData, data.hierarchy)
	}
}
