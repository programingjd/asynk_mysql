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

  override suspend fun aClose() {
    try {
      send(Packet.Quit())
    }
    finally {
      channel.close()
    }
  }

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
    send(Packet.StatementExecute(preparedStatement.id, preparedStatement.types, params))
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
    send(Packet.StatementExecute(preparedStatement.id, preparedStatement.types, params))
    val rs = receive(Packet.BinaryResultSet::class.java)
    val n = rs.columnCount
    val cols = ArrayList<Packet.ColumnDefinition>(n)
    for (i in 0 until n) {
      cols.add(receive(Packet.ColumnDefinition::class.java))
    }
    receive(Packet.EOF::class.java)
    val channel = Channel<Map<String, Any?>>()
    launch(EmptyCoroutineContext) {
      while (true) {
        val row = receive(Packet.Row::class.java)
        if (row.bytes == null) break
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
    val types = (1..prepareOK.paramCount).map { receive(Packet.ColumnDefinition::class.java) }
    if (prepareOK.paramCount > 0) receive(Packet.EOF::class.java)
    /*val cols =*/ (1..prepareOK.columnCount).map { receive(Packet.ColumnDefinition::class.java) }
    if (prepareOK.columnCount > 0) receive(Packet.EOF::class.java)
    return MysqlPreparedStatement(prepareOK.statementId, types, temporary)
  }

  internal suspend fun send(packet: Packet.FromClient) {
    if (buffer.limit() != buffer.position()) {
      val eof = receive(Packet.EOF::class.java)
      println(eof)
      assert(buffer.limit() == buffer.position())
    }
    packet.writeTo(buffer.clear() as ByteBuffer)
    channel.aWrite(buffer.flip() as ByteBuffer, 5000L, TimeUnit.MILLISECONDS)
    buffer.clear().flip()
  }

  internal suspend fun <T: Packet.FromServer> receive(type: Class<T>): T {
    if (buffer.remaining() > 0) {
      if (buffer.remaining() > 4) {
        val packet = Packet.fromBytes(buffer, type)
        if (packet != null) return packet
      }
    }
    buffer.compact()
    val left = buffer.capacity() - buffer.position()
    val n = channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
    if (n == left) throw RuntimeException("Connection buffer too small.")
    buffer.flip()
    try {
      return Packet.fromBytes(buffer, type) ?: throw RuntimeException("Connection buffer too small.")
    }
    catch (e: Packet.Exception) {
      if (e.message != null) err(e.message)
      throw RuntimeException(e)
    }
  }

  inner class MysqlPreparedStatement internal constructor(
                                     internal val id: Int,
                                     internal val types: List<Packet.ColumnDefinition>,
                                     internal val temporary: Boolean): PreparedStatement {
    override suspend fun rows() = this@MysqlConnection.rows(this)
    override suspend fun rows(params: Iterable<Any?>) = this@MysqlConnection.rows(this, params)
    override suspend fun affectedRows() = this@MysqlConnection.affectedRows(this)
    override suspend fun affectedRows(
      params: Iterable<Any?>
    ) = this@MysqlConnection.affectedRows(this, params)
    override suspend fun aClose() = this@MysqlConnection.close(this)
  }

  class MysqlResultSet internal constructor(
                       private val channel: Channel<Map<String, Any?>>): Connection.ResultSet {
    override operator fun iterator() = channel.iterator()
    override fun close() { channel.cancel() }
    override suspend fun toList() = channel.toList()
  }

  companion object {
    suspend fun to(
      database: String,
      credentials: MysqlAuthentication.Credentials = MysqlAuthentication.Credentials.UnsecuredCredentials(),
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 5432),
      bufferSize: Int = 4194304 // needs to hold any RowData message
    ): MysqlConnection {
      if (bufferSize < 1024) throw IllegalArgumentException(
        "Buffer size ${bufferSize} is smaller than the minimum buffer size of 1024")
      if (bufferSize > 16777215) throw IllegalArgumentException(
        "Buffer size ${bufferSize} is greater than the maximum buffer size of 16777215")
      val channel = AsynchronousSocketChannel.open()
      try {
        channel.aConnect(address)
        val buffer = ByteBuffer.allocateDirect(bufferSize)
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
