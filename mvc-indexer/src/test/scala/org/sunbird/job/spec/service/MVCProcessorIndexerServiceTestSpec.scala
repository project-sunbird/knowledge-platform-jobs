package org.sunbird.job.spec.service

import com.datastax.driver.core.Row
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.{ArgumentCaptor, Mockito}
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.mvcindexer.service.MVCIndexerService
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.util.{CassandraUtil, ElasticSearchUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.ArgumentMatchers.{any, endsWith}
import org.mockito.Mockito.{doNothing, times, verify, when}
import org.sunbird.job.Metrics
import org.sunbird.job.exception.{APIException, CassandraException, ElasticSearchException}

import java.util

class MVCProcessorIndexerServiceTestSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  lazy val jobConfig: MVCIndexerConfig = new MVCIndexerConfig(config)
  var cassandraUtil: CassandraUtil = _
  var mockElasticUtil:ElasticSearchUtil = _
  var mockHttpUtil:HttpUtil = _
  var mockMetrics: Metrics = _
//  val mockHttpUtil:HttpUtil = new HttpUtil
  var mvcProcessorIndexer: MVCIndexerService = _

  val contentResponse = """{"responseCode":"OK","result":{"content":{"channel":"in.ekstep","framework":"NCF","name":"Ecml bundle Test","language":["English"],"appId":"dev.sunbird.portal","contentEncoding":"gzip","identifier":"do_112806963140329472124","mimeType":"application/vnd.ekstep.ecml-archive","contentType":"Resource","objectType":"Content","artifactUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_112806963140329472124/artifact/1563350021721_do_112806963140329472124.zip","previewUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/ecml/do_112806963140329472124-latest","streamingUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/ecml/do_112806963140329472124-latest","downloadUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_112806963140329472124/ecml-bundle-test_1563350022377_do_112806963140329472124_1.0.ecar","status":"Live","pkgVersion":1,"lastUpdatedOn":"2019-07-17T07:53:25.618+0000"}}}"""

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.lmsDbHost, jobConfig.lmsDbPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    testCassandraUtil(cassandraUtil)
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    testCassandraUtil(cassandraUtil)
    mockElasticUtil = mock[ElasticSearchUtil](Mockito.withSettings().serializable())
    mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
    mockMetrics = mock[Metrics](Mockito.withSettings().serializable())
    mvcProcessorIndexer = new MVCIndexerService(jobConfig, mockElasticUtil, mockHttpUtil, cassandraUtil)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
  }

  "MVCProcessorIndexerService" should "generate es log" in {
    val argumentCaptor = ArgumentCaptor.forClass(classOf[ElasticSearchUtil]).asInstanceOf[ArgumentCaptor[String]]
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)

    when(mockHttpUtil.get(endsWith("content/v3/read/do_112806963140329472124"), any())).thenReturn(HTTPResponse(200, contentResponse))
    when(mockHttpUtil.post(endsWith("/daggit/submit"), any(), any())).thenReturn(HTTPResponse(200, """{}"""))

    mvcProcessorIndexer.processMessage(inputEvent)(mockMetrics)

    val insertedRecord = readFromCassandra(inputEvent.identifier)
    insertedRecord.forEach(col => {
      col.getObject("source") should be ("Sunbird 1")
      col.getObject("sourceurl") should be ("https://dev.sunbirded.org/play/content/do_112806963140329472124")
      col.getObject("level1_name").asInstanceOf[util.ArrayList[String]] should contain("Math-Magic")
      col.getObject("textbook_name").asInstanceOf[util.ArrayList[String]] should contain("How Many Times?")
      col.getObject("level1_concept").asInstanceOf[util.ArrayList[String]] should contain("Addition")
    })

    verify(mockElasticUtil, times(1)).addDocumentWithIndex(argumentCaptor.capture(), argumentCaptor.capture(), argumentCaptor.capture());
    val esRecordMap = JSONUtil.deserialize[Map[String, AnyRef]](argumentCaptor.getAllValues.get(0))

    esRecordMap("level1Concept").asInstanceOf[List[String]] should contain("Addition")
    esRecordMap("artifactUrl") should be("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_112806963140329472124/artifact/1563350021721_do_112806963140329472124.zip")
    esRecordMap("downloadUrl") should be("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_112806963140329472124/ecml-bundle-test_1563350022377_do_112806963140329472124_1.0.ecar")
    esRecordMap("pkgVersion") should be(1)
    esRecordMap("previewUrl") should be("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/ecml/do_112806963140329472124-latest")
    esRecordMap("level1Name").asInstanceOf[List[String]] should contain("Math-Magic")
    esRecordMap("identifier") should be("do_112806963140329472124")
    esRecordMap("appId") should be("dev.sunbird.portal")
    esRecordMap("objectType") should be("Content")
    esRecordMap("name") should be("Ecml bundle Test")
    esRecordMap("lastUpdatedOn") should be("2019-07-17T07:53:25.618+0000")
    esRecordMap("status") should be("Live")
    esRecordMap("source").asInstanceOf[List[String]] should contain("Sunbird 1")
    esRecordMap("contentEncoding") should be("gzip")
    esRecordMap("contentType") should be("Resource")
    esRecordMap("channel") should be("in.ekstep")
    esRecordMap("textbook_name").asInstanceOf[List[String]] should contain("How Many Times?")
    esRecordMap("mimeType") should be("application/vnd.ekstep.ecml-archive")
    esRecordMap("language").asInstanceOf[List[String]] should contain("English")
    esRecordMap("sourceURL") should be("https://dev.sunbirded.org/play/content/do_112806963140329472124")
    argumentCaptor.getAllValues.get(2) should be(inputEvent.identifier)
  }

  "MVCProcessorIndexerService" should "update ml-keywords" in {
    val argumentCaptor = ArgumentCaptor.forClass(classOf[ElasticSearchUtil]).asInstanceOf[ArgumentCaptor[String]]
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_3), 0, 11)

    when(mockHttpUtil.post(endsWith("/ml/vector/ContentText"), any(), any())).thenReturn(HTTPResponse(200, """{}"""))

    mvcProcessorIndexer.processMessage(inputEvent)(mockMetrics)

    verify(mockHttpUtil, times(1)).post(argumentCaptor.capture(), argumentCaptor.capture(), any());
    val esRecordMap = JSONUtil.deserialize[Map[String, AnyRef]](argumentCaptor.getAllValues.get(1))

    esRecordMap("request").asInstanceOf[Map[String, AnyRef]]("text").asInstanceOf[List[String]] should contain(inputEvent.mlContentText)
    esRecordMap("request").asInstanceOf[Map[String, AnyRef]]("cid") should be(inputEvent.identifier)

    val insertedRecord = readFromCassandra(inputEvent.identifier)
    insertedRecord.forEach(col => {
      val mlKeywords = col.getObject("ml_keywords").asInstanceOf[util.ArrayList[String]]
      mlKeywords should contain(inputEvent.mlKeywords(0))
      mlKeywords should contain(inputEvent.mlKeywords(1))
      mlKeywords should contain(inputEvent.mlKeywords(2))
      col.getObject("ml_content_text") should be(inputEvent.mlContentText)
    })
  }

  "MVCProcessorIndexerService" should "update ml-contenttextvector" in {
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_4), 0, 11)

    when(mockHttpUtil.get(endsWith("/ml/vector/ContentText"), any())).thenReturn(HTTPResponse(200, contentResponse))

    mvcProcessorIndexer.processMessage(inputEvent)(mockMetrics)

    val insertedRecord = readFromCassandra(inputEvent.identifier)
    insertedRecord.forEach(col => {
      val mlContentTextVector = col.getObject("ml_content_text_vector").asInstanceOf[util.LinkedHashSet[Double]]
      mlContentTextVector should contain(inputEvent.mlContentTextVector(0))
      mlContentTextVector should contain(inputEvent.mlContentTextVector(1))
      mlContentTextVector should contain(inputEvent.mlContentTextVector(2))
      mlContentTextVector should contain(inputEvent.mlContentTextVector(3))
    })
  }

  "MVCProcessorIndexerService" should "update content-ratings" in {
    val argumentCaptor = ArgumentCaptor.forClass(classOf[ElasticSearchUtil]).asInstanceOf[ArgumentCaptor[String]]
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_5), 0, 11)

    when(mockElasticUtil.getDocumentAsString(any())).thenReturn("""{"_id":"do_112806963140329472124"}""")

    mvcProcessorIndexer.processMessage(inputEvent)(mockMetrics)

    verify(mockElasticUtil, times(1)).updateDocument(argumentCaptor.capture(), argumentCaptor.capture());
    val esRecordMap = JSONUtil.deserialize[Map[String, AnyRef]](argumentCaptor.getAllValues.get(1))

    esRecordMap("me_total_time_spent_in_portal") should be(inputEvent.metadata("me_total_time_spent_in_portal"))
    esRecordMap("me_averageRating") should be(inputEvent.metadata("me_averageRating"))
    esRecordMap("me_total_time_spent_in_app") should be(inputEvent.metadata("me_total_time_spent_in_app"))
    esRecordMap("me_total_play_sessions_in_portal") should be(inputEvent.metadata("me_total_play_sessions_in_portal"))
    esRecordMap("me_total_play_sessions_in_desktop") should be(inputEvent.metadata("me_total_play_sessions_in_desktop"))
    esRecordMap("me_total_play_sessions_in_app") should be(inputEvent.metadata("me_total_play_sessions_in_app"))
    esRecordMap("me_total_time_spent_in_desktop") should be(inputEvent.metadata("me_total_time_spent_in_desktop"))
  }

  "MVCProcessorIndexerService" should "increase Es failed metric on when exception in ElasticSearch" in {
    val argumentCaptor = ArgumentCaptor.forClass(classOf[ElasticSearchUtil]).asInstanceOf[ArgumentCaptor[String]]
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1),0, 10)

    when(mockHttpUtil.get(endsWith("content/v3/read/do_112806963140329472124"), any())).thenReturn(HTTPResponse(200, contentResponse))
    when(mockHttpUtil.post(endsWith("/daggit/submit"), any(), any())).thenReturn(HTTPResponse(200, """{}"""))

    try {
      mvcProcessorIndexer.processMessage(inputEvent)(mockMetrics)
    } catch {
      case ex: ElasticSearchException => {
        verify(mockMetrics, times(2)).incCounter(argumentCaptor.capture());
        argumentCaptor.getAllValues.get(0) should be(jobConfig.dbUpdateCount)
        argumentCaptor.getAllValues.get(1) should be(jobConfig.esFailedEventCount)
      }
    }
  }

  "MVCProcessorIndexerService" should "increase DB failed metric on while exception in Cassandra" in {
    val argumentCaptor = ArgumentCaptor.forClass(classOf[ElasticSearchUtil]).asInstanceOf[ArgumentCaptor[String]]
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_3), 0, 11)

    when(mockHttpUtil.post(endsWith("/ml/vector/ContentText"), any(), any())).thenReturn(HTTPResponse(200, """{}"""))

    try {
      cassandraUtil.close()
      mvcProcessorIndexer.processMessage(inputEvent)(mockMetrics)
    } catch {
      case ex: CassandraException => {
        verify(mockMetrics, times(1)).incCounter(argumentCaptor.capture());
        argumentCaptor.getAllValues.get(0) should be(jobConfig.dbUpdateFailedCount)
      }
    }
  }

  "MVCProcessorIndexerService" should "increase api failed metric on while exception in API" in {
    val argumentCaptor = ArgumentCaptor.forClass(classOf[ElasticSearchUtil]).asInstanceOf[ArgumentCaptor[String]]
    val inputEvent:Event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_3), 0, 11)

    when(mockHttpUtil.post(endsWith("/ml/vector/ContentText"), any(), any())).thenReturn(HTTPResponse(500, """{}"""))

    try {
      mvcProcessorIndexer.processMessage(inputEvent)(mockMetrics)
    } catch {
      case ex: APIException => {
        verify(mockMetrics, times(1)).incCounter(argumentCaptor.capture());
        argumentCaptor.getAllValues.get(0) should be(jobConfig.apiFailedEventCount)
      }
    }
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }

  def readFromCassandra(contentId: String): util.List[Row] = {
    val query = s"select * from ${jobConfig.dbKeyspace}.${jobConfig.dbTable} where content_id='${contentId}' ALLOW FILTERING;"
    cassandraUtil.find(query)
  }
}
