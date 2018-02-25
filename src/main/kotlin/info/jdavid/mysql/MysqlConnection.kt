package info.jdavid.mysql

import info.jdavid.sql.Connection
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.toList
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.EmptyCoroutineContext
typealias PreparedStatement= Connection.PreparedStatement<MysqlConnection>


class MysqlConnection internal constructor(private val channel: AsynchronousSocketChannel,
                                           private val buffer: ByteBuffer): Connection<MysqlConnection> {

  override fun close() = channel.close()

  override suspend fun affectedRows(sqlStatement: String) = affectedRows(sqlStatement, emptyList())

  override suspend fun affectedRows(sqlStatement: String, params: Iterable<Any?>): Int {
    val statement = prepare(sqlStatement, true)
    return affectedRows(statement, params)
  }

  override suspend fun affectedRows(preparedStatement: PreparedStatement) = affectedRows(
    preparedStatement, emptyList()
  )

  override suspend fun affectedRows(preparedStatement: PreparedStatement, params: Iterable<Any?>): Int {
    if (preparedStatement !is MysqlPreparedStatement) throw IllegalArgumentException()
    send(Packet.StatementExecute(preparedStatement.id, params))
    val ok = receive(Packet.OK::class.java)
    return ok.count
  }

  override suspend fun rows(sqlStatement: String) = rows(sqlStatement, emptyList())

  override suspend fun rows(sqlStatement: String, params: Iterable<Any?>): MysqlResultSet {
    val statement = prepare(sqlStatement, true)
    return rows(statement, params)
  }

  override suspend fun rows(preparedStatement: PreparedStatement) = rows(preparedStatement, emptyList())

  override suspend fun rows(preparedStatement: PreparedStatement, params: Iterable<Any?>): MysqlResultSet {
    if (preparedStatement !is MysqlPreparedStatement) throw IllegalArgumentException()
    send(Packet.StatementExecute(preparedStatement.id, params))
    val rs = receive(Packet.BinaryResultSet::class.java)
    val n = rs.columnCount
    val cols = ArrayList<Packet.ColumnDefinition>(n)
    for (i in 0 until n) {
      cols.add(receive(Packet.ColumnDefinition::class.java))
    }
    val channel = Channel<Map<String, Any?>>()
    launch(EmptyCoroutineContext) {
      while (true) {
        val row = receive(Packet.Row::class.java)
        if (row.bytes == null) break
        println(row)
        channel.send(row.decode(cols))
      }
      if (preparedStatement.temporary) {
        send(Packet.StatementReset(preparedStatement.id))
        /*val ok =*/ receive(Packet.OK::class.java)
      }
      channel.close()
    }
    return MysqlResultSet(channel)
  }

  suspend fun close(preparedStatement: MysqlPreparedStatement) {
    send(Packet.StatementClose(preparedStatement.id))
  }

  override suspend fun prepare(sqlStatement: String): MysqlPreparedStatement {
    return prepare(sqlStatement, false)
  }

  private suspend fun prepare(sqlStatement: String, temporary: Boolean): MysqlPreparedStatement {
    send(Packet.StatementPrepare(sqlStatement))
    val prepareOK = receive(Packet.StatementPrepareOK::class.java)
    for (i in 1..prepareOK.columnCount) receive(Packet.ColumnDefinition::class.java)
    for (i in 1..prepareOK.paramCount) receive(Packet.ColumnDefinition::class.java)
    return MysqlPreparedStatement(prepareOK.statementId, temporary)
  }

  internal suspend fun send(packet: Packet.FromClient) {
    assert(buffer.limit() == buffer.position())
    packet.writeTo(buffer.clear() as ByteBuffer)
    channel.aWrite(buffer.flip() as ByteBuffer, 5000L, TimeUnit.MILLISECONDS)
    buffer.clear().flip()
  }

  internal suspend fun <T: Packet.FromServer> receive(type: Class<T>): T {
    if (buffer.remaining() > 0) return Packet.fromBytes(buffer, type)
    buffer.compact()
    val left = buffer.capacity() - buffer.position()
    val n = channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
    if (n == left) throw RuntimeException("Connection buffer too small.")
    buffer.flip()
    try {
      return Packet.fromBytes(buffer, type)
    }
    catch (e: Packet.Exception) {
      if (e.message != null) err(e.message)
      throw RuntimeException(e)
    }
  }

  inner class MysqlPreparedStatement internal constructor(
                                     internal val id: Int,
                                     internal val temporary: Boolean): PreparedStatement {
    override suspend fun rows() = this@MysqlConnection.rows(this)
    override suspend fun rows(params: Iterable<Any?>) = this@MysqlConnection.rows(this, params)
    override suspend fun affectedRows() = this@MysqlConnection.affectedRows(this)
    override suspend fun affectedRows(
      params: Iterable<Any?>
    ) = this@MysqlConnection.affectedRows(this, params)
    override suspend fun close() = this@MysqlConnection.close(this)
  }

  class MysqlResultSet(private val channel: Channel<Map<String, Any?>>): Connection.ResultSet {
    override operator fun iterator() = channel.iterator()
    override fun close() { channel.cancel() }
    override suspend fun toList() = channel.toList()
  }

  companion object {
    suspend fun to(
      database: String,
      credentials: MysqlAuthentication.Credentials = MysqlAuthentication.Credentials.UnsecuredCredentials(),
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 5432)
    ): MysqlConnection {
      val channel = AsynchronousSocketChannel.open()
      try {
        channel.aConnect(address)
        val buffer = ByteBuffer.allocateDirect(4194304)// needs to hold any RowData message
        buffer.order(ByteOrder.LITTLE_ENDIAN).flip()
        val connection = MysqlConnection(channel, buffer)
        MysqlAuthentication.authenticate(connection, database, credentials)
        return connection
      }
      catch (e: Exception) {
        channel.close()
        throw e
      }
    }
    private val logger = LoggerFactory.getLogger(MysqlConnection::class.java)
//    private fun warn(message: String) = logger.warn(message)
    private fun err(message: String) = logger.error(message)

    internal fun hex(bytes: ByteArray): String {
      val chars = CharArray(bytes.size * 2)
      var i = 0
      for (b in bytes) {
        chars[i++] = Character.forDigit(b.toInt().shr(4).and(0xf), 16)
        chars[i++] = Character.forDigit(b.toInt().and(0xf), 16)
      }
      return String(chars)
    }
  }

}
