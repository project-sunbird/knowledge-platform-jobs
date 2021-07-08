package org.sunbird.job.spec

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.qrimagegenerator.domain.Event
import org.sunbird.job.qrimagegenerator.task.{QRCodeImageGeneratorConfig, QRCodeImageGeneratorTask}
import org.sunbird.job.util.CloudStorageUtil
import org.sunbird.job.util.{CassandraUtil, JSONUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

class QRCodeImageGeneratorTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: QRCodeImageGeneratorConfig = new QRCodeImageGeneratorConfig(config)
  val cloudStorageUtil:CloudStorageUtil = new CloudStorageUtil(jobConfig.cloudStorageType, jobConfig.storageKey, jobConfig.storageSecret)
  var cassandraUtils: CassandraUtil = _

  var currentMilliSecond = 1605816926271L

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtils = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort)
    val session = cassandraUtils.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
    flinkCluster.before()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    flinkCluster.after()
    super.afterAll()
  }

  "QRCodeImageGeneratorTask" should "generate event" in {
    when(mockKafkaUtil.kafkaJobRequestSource[Event](jobConfig.kafkaInputTopic)).thenReturn(new QRCodeImageGeneratorMapSource)
    new QRCodeImageGeneratorTask(jobConfig, mockKafkaUtil).process()
//    assertThrows[JobExecutionException] {
//      new QRCodeImageGeneratorTask(jobConfig, mockKafkaUtil).process()
//    }
    
  }
}

class QRCodeImageGeneratorMapSource extends SourceFunction[Event] {

  override def run(ctx: SourceContext[Event]) {
    // Valid event
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1), 0, 10))
    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2), 0, 11))
    // Invalid event
//    ctx.collect(new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_4)))
  }

  override def cancel(): Unit = {}
}