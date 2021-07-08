package org.sunbird.job.mvcindexer.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.mvcindexer.functions.MVCIndexer
import org.sunbird.job.util.{ElasticSearchUtil, FlinkUtil, HttpUtil}

import java.io.File


class MVCIndexerStreamTask(config: MVCIndexerConfig, kafkaConnector: FlinkKafkaConnector, esUtil: ElasticSearchUtil, httpUtil: HttpUtil) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new MVCIndexer(config, esUtil, httpUtil))
      .name(config.mvcIndexerFunction)
      .uid(config.mvcIndexerFunction)
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object MVCIndexerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("mvc-indexer.conf").withFallback(ConfigFactory.systemEnvironment()))
    val MVCIndexerConfig = new MVCIndexerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(MVCIndexerConfig)
    val esUtil: ElasticSearchUtil = null
    val httpUtil = new HttpUtil
    val task = new MVCIndexerStreamTask(MVCIndexerConfig, kafkaUtil, esUtil, httpUtil)
    task.process()
  }
}

// $COVERAGE-ON$
