package org.sunbird.job.spec

import com.typesafe.config.{Config, ConfigFactory}
import okhttp3.mockwebserver.{Dispatcher, MockResponse, MockWebServer, RecordedRequest}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.sunbird.job.util.{CassandraUtil, ElasticSearchUtil, HttpUtil, JSONUtil}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.mvcindexer.task.{MVCIndexerConfig, MVCIndexerStreamTask}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

import java.util

class MVCProcessorIndexerTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: MVCIndexerConfig = new MVCIndexerConfig(config)
  var cassandraUtil: CassandraUtil = null
  val esUtil: ElasticSearchUtil = null
  val httpUtil: HttpUtil = new HttpUtil
  val esServer = new MockWebServer()

  var currentMilliSecond = 1605816926271L

  val esDispatcher: Dispatcher = new Dispatcher() {
    @throws[InterruptedException]
    override def dispatch(request: RecordedRequest): MockResponse = {
      (request.getPath, request.getMethod) match {
        case ("/mvc-content-v1", "HEAD") =>
          new MockResponse().setResponseCode(200)
        case ("/mvc-content-v1?master_timeout=30s&timeout=30s", "PUT") =>
          new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"acknowledged":true,"shards_acknowledged":true,"index":"mvc-content-v1"}""")
        case ("/mvc-content-v1/_doc/do_112806963140329472124?timeout=1m", "PUT") =>
          new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"_index":"mvc-content-v1","_type":"_doc","_id":"do_112806963140329472124","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}""")
        case _ => {
          new MockResponse().setResponseCode(200)
        }
      }
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    esServer.setDispatcher(esDispatcher)
    esServer.start(9200)

    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.lmsDbHost, jobConfig.lmsDbPort)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session);
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true));
    testCassandraUtil(cassandraUtil)
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    super.afterAll()
    esServer.close()
    flinkCluster.after()
  }


  "MVCProcessorIndexerStreamTask" should "index in ES and Cassandra" in {
    val contentServer = new MockWebServer()
    contentServer.start(8080)
    contentServer.enqueue(new MockResponse().setHeader(
      "Content-Type", "application/json"
    ).setBody("""{"responseCode":"OK","result":{"content":{"channel":"in.ekstep","framework":"NCF","name":"Ecml bundle Test","language":["English"],"appId":"dev.sunbird.portal","contentEncoding":"gzip","identifier":"do_112806963140329472124","mimeType":"application/vnd.ekstep.ecml-archive","contentType":"Resource","objectType":"Content","artifactUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_112806963140329472124/artifact/1563350021721_do_112806963140329472124.zip","previewUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/ecml/do_112806963140329472124-latest","streamingUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/ecml/do_112806963140329472124-latest","downloadUrl":"https://sunbirddev.blob.core.windows.net/sunbird-content-dev/ecar_files/do_112806963140329472124/ecml-bundle-test_1563350022377_do_112806963140329472124_1.0.ecar","status":"Live","pkgVersion":1,"lastUpdatedOn":"2019-07-17T07:53:25.618+0000"}}}"""))

    val mlVectorServer = new MockWebServer()
    mlVectorServer.start(3579)

    mlVectorServer.enqueue(new MockResponse().setHeader(
      "Content-Type", "application/json"
    ).setBody("""{"responseCode":"OK"}"""))

    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new MVCProcessorIndexerMapSource)

    new MVCIndexerStreamTask(jobConfig, mockKafkaUtil, esUtil, httpUtil).process()

    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(2)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.dbUpdateCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.successEventCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(1)
    BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(0)
    contentServer.close()
  }

  "MVCProcessorIndexerStreamTask" should "throw exception and increase error count" in {
    val contentServer = new MockWebServer()
    contentServer.start(8080)
    contentServer.enqueue(new MockResponse().setHeader(
      "Content-Type", "application/json"
    ).setResponseCode(500).setBody("""{}"""))

    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new MVCProcessorIndexerMapSource)

    try {
      new MVCIndexerStreamTask(jobConfig, mockKafkaUtil, esUtil, httpUtil).process()
    } catch {
      case ex: JobExecutionException =>
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.totalEventsCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.failedEventCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.apiFailedEventCount}").getValue() should be(1)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.esFailedEventCount}").getValue() should be(0)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.dbUpdateFailedCount}").getValue() should be(0)
        BaseMetricsReporter.gaugeMetrics(s"${jobConfig.jobName}.${jobConfig.skippedEventCount}").getValue() should be(0)
    }
    contentServer.close()
  }

  def testCassandraUtil(cassandraUtil: CassandraUtil): Unit = {
    cassandraUtil.reconnect()
  }
}

class MVCProcessorIndexerMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    // Valid event
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 10))
    // Invalid event
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2), 0, 11))
  }

  override def cancel(): Unit = {}
}