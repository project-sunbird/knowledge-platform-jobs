package org.sunbird.job.publish.helpers.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.util.CloudStorageUtil
import org.sunbird.job.questionset.publish.helpers.QuestionSetPublisher
import org.sunbird.job.questionset.task.QuestionSetPublishConfig
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

class QuestionSetPublisherSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val jobConfig: QuestionSetPublishConfig = new QuestionSetPublishConfig(config)
  implicit val cloudStorageUtil = new CloudStorageUtil(jobConfig)
  implicit val readerConfig: ExtDataConfig = ExtDataConfig(jobConfig.questionSetKeyspaceName, jobConfig.questionSetTableName, List("identifier"), Map("hierarchy"->"string","instructions"->"string"))
  val questionReaderConfig: ExtDataConfig = ExtDataConfig(jobConfig.questionKeyspaceName, jobConfig.questionTableName)
  implicit val defCache = new DefinitionCache()
  implicit val defConfig = DefinitionConfig(jobConfig.schemaSupportVersionMap, jobConfig.definitionBasePath)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
      delay(10000)
    } catch {
      case ex: Exception => {
      }
    }
  }

  "validateQuestionSet with no hierarchy, mimeType and visibility" should "return 3 error message" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]))
    val result: List[String] = new TestQuestionSetPublisher().validateQuestionSet(data, data.identifier)
    result.size should be(3)
  }

  "saveExternalData" should "save external data like hierarchy" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Content Name", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]))
    new TestQuestionSetPublisher().saveExternalData(data, readerConfig)
  }

  "populateChildrenMapRecursively with two children" should "return a map with one data" in {
    val result: Map[String, AnyRef] = new TestQuestionSetPublisher().populateChildrenMapRecursively(List(Map[String, AnyRef] ("identifier" -> "do_123", "objectType" -> "Question"), Map[String, AnyRef] ("identifier" -> "do_124", "objectType" -> "QuestionSet")), Map())
    result.size should be (1)
  }

  "getDataForEcar with empty children" should "return one element in list" in {
    val data = new ObjectData("do_123", Map(), Some(Map()), Some(Map[String, AnyRef]("identifier" -> "do_123", "children" -> List(Map()))))
    val result: Option[List[Map[String, AnyRef]]] = new TestQuestionSetPublisher().getDataForEcar(data)
    result.size should be (1)
  }

  "enrichObjectMetadata with valid children" should "return updated pkgVersion" in {
    val data = new ObjectData("do_123", Map[String, AnyRef] ("name" -> "QS1"), Some(Map()), Some(Map[String, AnyRef]("identifier" -> "do_123", "children" -> List(Map[String, AnyRef]("identifier" -> "do_124", "objectType"->"QuestionSet", "visibility"-> "Parent"), Map[String, AnyRef]("identifier" -> "do_113188615625731", "objectType"->"Question", "visibility"-> "Default")))))
    val result: Option[ObjectData] = new TestQuestionSetPublisher().enrichObjectMetadata(data)
    result.getOrElse(new ObjectData("do_123", Map())).pkgVersion should be (1.asInstanceOf[Number])
    result.size should be (1)
  }

  "getHierarchy " should " return the hierarchy of the Question Set for the image id " in {
    val identifier = "do_123"
    val result =  new TestQuestionSetPublisher().getHierarchy(identifier, 1.0, readerConfig)
    result.getOrElse(Map()).size should be(3)
    result.getOrElse(Map()).getOrElse("children", List(Map())).asInstanceOf[List[Map[String, AnyRef]]].size should be (2)
  }

  "getHierarchy " should " return the hierarchy of the Question Set " in {
    val identifier = "do_321"
    val result =  new TestQuestionSetPublisher().getHierarchy(identifier, 1.0, readerConfig)
    result.getOrElse(Map()).size should be(3)
    result.getOrElse(Map()).getOrElse("children", List(Map())).asInstanceOf[List[Map[String, AnyRef]]].size should be (1)
  }

  "getQuestions " should " return the question data from the hierarchy of the Question Set " in {
    val data = new ObjectData("do_123", Map[String, AnyRef] ("name" -> "QS1"), Some(Map()), Some(Map[String, AnyRef]("identifier" -> "do_123", "children" -> List(Map[String, AnyRef]("identifier" -> "do_113188615625731", "objectType"->"Question", "visibility"-> "Parent")))))
    val result =  new TestQuestionSetPublisher().getQuestions(data, questionReaderConfig)
    result.size should be(1)
  }

  "getExtDatas " should " return the External data of the provided identifiers " in {
    val result =  new TestQuestionSetPublisher().getExtDatas(List("do_113188615625731", "do_113188615625731.img"), questionReaderConfig)
    result.getOrElse(List(Map())).size should be(2)
  }

  "deleteExternalData with valid ObjectData" should "should do nothing" in {
    new TestQuestionSetPublisher().deleteExternalData(new ObjectData("do_123", Map()), readerConfig)
  }

  "deleteExternalData " should "do nothing " in {
    val objData = new ObjectData("do_113188615625731", Map());
    new TestQuestionSetPublisher().deleteExternalData(objData, readerConfig)
  }

  "getExtData " should " return ObjectExtData having hierarchy" in {
    val identifier = "do_321"
    val result =  new TestQuestionSetPublisher().getExtData(identifier, 1.0, readerConfig)
    result.getOrElse(new ObjectExtData).asInstanceOf[ObjectExtData].hierarchy.getOrElse(Map()).contains("do_321")
    result.getOrElse(new ObjectExtData).asInstanceOf[ObjectExtData].hierarchy.getOrElse(Map()).getOrElse("children", List(Map())).asInstanceOf[List[Map[String, AnyRef]]].size should be (1)
  }
  "getHierarchies " should "do nothing " in {
    val identifier = "do_113188615625731";
    new TestQuestionSetPublisher().getHierarchies(List(identifier), readerConfig)
  }

  def delay(time: Long): Unit = {
    try {
      Thread.sleep(time)
    } catch {
      case ex: Exception => print("")
    }
  }
}

class TestQuestionSetPublisher extends QuestionSetPublisher{}