package org.sunbird.job.exception

class APIException(message: String, cause: Throwable) extends Exception(message, cause)

class CassandraException(message: String, cause: Throwable) extends Exception(message, cause)

class ElasticSearchException(message: String, cause: Throwable) extends Exception(message, cause)
