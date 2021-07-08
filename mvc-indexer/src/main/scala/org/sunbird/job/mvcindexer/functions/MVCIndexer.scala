package org.sunbird.job.mvcindexer.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.mvcindexer.service.MVCIndexerService
import org.sunbird.job.mvcindexer.task.MVCIndexerConfig
import org.sunbird.job.util.{CassandraUtil, ElasticSearchUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

class MVCIndexer(config: MVCIndexerConfig, var esUtil: ElasticSearchUtil, httpUtil: HttpUtil)
                (implicit eventTypeInfo: TypeInformation[Event],
                 @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config) {

  var mvcIndexerService: MVCIndexerService = _

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.successEventCount, config.failedEventCount, config.esFailedEventCount, config.skippedEventCount, config.dbUpdateCount, config.dbUpdateFailedCount, config.apiFailedEventCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (esUtil == null) {
      esUtil = new ElasticSearchUtil(config.esConnectionInfo, config.mvcProcessorIndex, config.mvcProcessorIndexType)
    }
    cassandraUtil = new CassandraUtil(config.lmsDbHost, config.lmsDbPort)
    mvcIndexerService = new MVCIndexerService(config, esUtil, httpUtil, cassandraUtil)
  }

  override def close(): Unit = {
    esUtil.close()
    mvcIndexerService.closeConnection()
    super.close()
  }

  @throws(classOf[InvalidEventException])
  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    try {
      metrics.incCounter(config.totalEventsCount)
      if (event.isValid) {
        mvcIndexerService.processMessage(event)(metrics)
      } else metrics.incCounter(config.skippedEventCount)
    } catch {
      case ex: Exception =>
        metrics.incCounter(config.failedEventCount)
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
    }
  }
}
