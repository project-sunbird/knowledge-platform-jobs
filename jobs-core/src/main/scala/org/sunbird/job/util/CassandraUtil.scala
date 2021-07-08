package org.sunbird.job.util

import java.util

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException

class CassandraUtil(host: String, port: Int) {

  val cluster = {
    Cluster.builder()
      .addContactPoints(host)
      .withPort(port)
      .withoutJMXReporting()
      .build()
  }
  var session = cluster.connect()

  def findOne(query: String): Row = {
    try {
      val rs: ResultSet = session.execute(query)
      rs.one
    } catch {
      case ex: DriverException =>
        this.reconnect()
        this.findOne(query)
    }
  }

  def find(query: String): util.List[Row] = {
    try {
      val rs: ResultSet = session.execute(query)
      rs.all
    } catch {
      case ex: DriverException =>
        this.reconnect()
        this.find(query)
    }
  }

  def upsert(query: String): Boolean = {
    val rs: ResultSet = session.execute(query)
    rs.wasApplied
  }

  def getUDTType(keyspace: String, typeName: String): UserType = session.getCluster.getMetadata.getKeyspace(keyspace).getUserType(typeName)

  def reconnect(): Unit = {
    this.session.close()
    val cluster: Cluster = Cluster.builder.addContactPoint(host).withPort(port).build
    this.session = cluster.connect
  }

  def close(): Unit = {
    this.session.close()
  }

  def update(query: Statement): Boolean = {
    val rs: ResultSet = session.execute(query)
    rs.wasApplied
  }

  def executeQuery(query: String): Unit = {
    session.execute(query)
  }

  def executePreparedStatement(query: String, params: Object*): util.List[Row] = {
    val rs: ResultSet = session.execute(session.prepare(query).bind(params : _*))
    rs.all()
  }
}
