package info.jdavid.asynk.mysql

import info.jdavid.asynk.sql.Connection
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.toCollection
import kotlinx.coroutines.experimental.channels.toList
import kotlinx.coroutines.experimental.channels.toMap
import kotlinx.coroutines.experimental.currentScope
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

typealias PreparedStatement= Connection.PreparedStatement<MysqlConnection>


/**
 * Mysql/MariaDB database connection.
 */
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

  override suspend fun commitTransaction() {
    send(Packet.Query("COMMIT"))
    receive(Packet.OK::class.java)
  }

  override suspend fun rollbackTransaction() {
    send(Packet.Query("ROLLBACK"))
    receive(Packet.OK::class.java)
  }

  override suspend fun startTransaction() {
    send(Packet.Query("START TRANSACTION"))
    receive(Packet.OK::class.java)
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

  override suspend fun <T> values(sqlStatement: String, columnNameOrAlias: String): MysqlResultSet<T> =
    values(sqlStatement, emptyList(), columnNameOrAlias)

  override suspend fun <K,V> entries(sqlStatement: String,
                                     keyColumnNameOrAlias: String,
                                     valueColumnNameOrAlias: String): MysqlResultMap<K,V> =
    entries(sqlStatement, emptyList(), keyColumnNameOrAlias, valueColumnNameOrAlias)

  override suspend fun rows(sqlStatement: String) = rows(sqlStatement, emptyList())

  override suspend fun <T> values(sqlStatement: String, params: Iterable<Any?>,
                                  columnNameOrAlias: String): MysqlResultSet<T> {
    val statement = prepare(sqlStatement, true)
    return values(statement, params, columnNameOrAlias)
  }

  override suspend fun <K,V> entries(sqlStatement: String, params: Iterable<Any?>,
                                      keyColumnNameOrAlias: String,
                                      valueColumnNameOrAlias: String): MysqlResultMap<K,V> {
    val statement = prepare(sqlStatement, true)
    return entries(statement, params, keyColumnNameOrAlias, valueColumnNameOrAlias)
  }

  override suspend fun rows(sqlStatement: String, params: Iterable<Any?>): MysqlResultSet<Map<String,Any?>> {
    val statement = prepare(sqlStatement, true)
    return rows(statement, params)
  }

  override suspend fun <T> values(preparedStatement: PreparedStatement,
                                  columnNameOrAlias: String): MysqlResultSet<T> =
    values(preparedStatement, emptyList(), columnNameOrAlias)

  override suspend fun <K,V> entries(preparedStatement: PreparedStatement,
                                     keyColumnNameOrAlias: String,
                                     valueColumnNameOrAlias: String): MysqlResultMap<K,V> =
    entries(preparedStatement, emptyList(), keyColumnNameOrAlias, valueColumnNameOrAlias)

  override suspend fun rows(preparedStatement: PreparedStatement) = rows(preparedStatement, emptyList())

  override suspend fun <T> values(preparedStatement: PreparedStatement,
                                  params: Iterable<Any?>,
                                  columnNameOrAlias: String): MysqlResultSet<T> {
    if (preparedStatement !is MysqlPreparedStatement) throw IllegalArgumentException()
    send(Packet.StatementExecute(preparedStatement.id, preparedStatement.types, params))
    val rs = receive(Packet.BinaryResultSet::class.java)
    val n = rs.columnCount
    val cols = ArrayList<Packet.ColumnDefinition>(n)
    for (i in 0 until n) {
      cols.add(receive(Packet.ColumnDefinition::class.java))
    }
    receive(Packet.EOF::class.java)
    val channel = Channel<T>()
    currentScope {
      launch {
        while (true) {
          val row = receive(Packet.Row::class.java)
          if (row.bytes == null) break
          channel.send(row.decode(cols, columnNameOrAlias))
        }
        if (preparedStatement.temporary) {
          send(Packet.StatementReset(preparedStatement.id))
          /*val ok =*/ receive(Packet.OK::class.java)
        }
        channel.close()
      }
    }
    return MysqlResultSet(channel)
  }

  override suspend fun <K,V> entries(preparedStatement: PreparedStatement,
                                     params: Iterable<Any?>,
                                     keyColumnNameOrAlias: String,
                                     valueColumnNameOrAlias: String): MysqlResultMap<K,V> {
    if (preparedStatement !is MysqlPreparedStatement) throw IllegalArgumentException()
    send(Packet.StatementExecute(preparedStatement.id, preparedStatement.types, params))
    val rs = receive(Packet.BinaryResultSet::class.java)
    val n = rs.columnCount
    val cols = ArrayList<Packet.ColumnDefinition>(n)
    for (i in 0 until n) {
      cols.add(receive(Packet.ColumnDefinition::class.java))
    }
    receive(Packet.EOF::class.java)
    val channel = Channel<Pair<K,V>>()
    currentScope {
      launch {
        while (true) {
          val row = receive(Packet.Row::class.java)
          if (row.bytes == null) break
          channel.send(row.decode(cols, keyColumnNameOrAlias, valueColumnNameOrAlias))
        }
        if (preparedStatement.temporary) {
          send(Packet.StatementReset(preparedStatement.id))
          /*val ok =*/ receive(Packet.OK::class.java)
        }
        channel.close()
      }
    }
    return MysqlResultMap(channel)
  }

  override suspend fun rows(preparedStatement: PreparedStatement,
                            params: Iterable<Any?>): MysqlResultSet<Map<String,Any?>> {
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
    currentScope {
      launch {
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
    }
    return MysqlResultSet(channel)
  }

  suspend fun close(preparedStatement: MysqlPreparedStatement) {
    send(Packet.StatementClose(preparedStatement.id))
  }

  override suspend fun prepare(sqlStatement: String) = prepare(sqlStatement, false)

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
      /*val eof =*/ receive(Packet.EOF::class.java)
      assert(buffer.limit() == buffer.position())
    }
    packet.writeTo(buffer.clear() as ByteBuffer)
    (buffer.flip() as ByteBuffer).apply {
      while (remaining() > 0) channel.aWrite(this, 5000L, TimeUnit.MILLISECONDS)
    }
    buffer.clear().flip()
  }

  internal suspend fun <T: Packet.FromServer> receive(type: Class<T>): T {
    if (buffer.remaining() > 0) {
      if (buffer.remaining() > 4) {
        val packet = Packet.fromBytes(buffer, type)
        if (packet != null) return packet
      }
    }
    try {
      while (true) {
        buffer.compact()
        val left = buffer.capacity() - buffer.position()
        val n = channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
        if (n == left) throw RuntimeException("Connection buffer too small.")
        buffer.flip()
        val packet = Packet.fromBytes(buffer, type)
        if (packet != null) return packet
        if (buffer.position() == buffer.capacity()) throw RuntimeException("Connection buffer too small.")
      }
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
    override suspend fun <T> values(columnNameOrAlias: String): MysqlResultSet<T> =
      this@MysqlConnection.values(this, columnNameOrAlias)
    override suspend fun <T> values(params: Iterable<Any?>, columnNameOrAlias: String): MysqlResultSet<T> =
      this@MysqlConnection.values(this, params, columnNameOrAlias)
    override suspend fun <K,V> entries(keyColumnNameOrAlias: String,
                                       valueColumnNameOrAlias: String): MysqlResultMap<K,V> =
      this@MysqlConnection.entries(this, keyColumnNameOrAlias, valueColumnNameOrAlias)
    override suspend fun <K,V> entries(params: Iterable<Any?>,
                                       keyColumnNameOrAlias: String,
                                       valueColumnNameOrAlias: String): MysqlResultMap<K,V> =
      this@MysqlConnection.entries(this, params, keyColumnNameOrAlias, valueColumnNameOrAlias)
    override suspend fun affectedRows() = this@MysqlConnection.affectedRows(this)
    override suspend fun affectedRows(
      params: Iterable<Any?>
    ) = this@MysqlConnection.affectedRows(this, params)
    override suspend fun aClose() = this@MysqlConnection.close(this)
  }

  open class MysqlResultSet<T> internal constructor(protected val channel: Channel<T>): Connection.ResultSet<T> {
    override operator fun iterator() = channel.iterator()
    override fun close() { channel.cancel() }
    override suspend fun toList() = use { channel.toList() }
    override suspend fun <C : MutableCollection<in T>> toCollection(destination: C) =
      use { channel.toCollection(destination) }
  }

  class MysqlResultMap<K,V> internal constructor(channel: Channel<Pair<K,V>>):
        MysqlResultSet<Pair<K,V>>(channel), Connection.ResultMap<K,V> {
    override suspend fun toMap() = use { channel.toMap() }
    override suspend fun <M : MutableMap<in K, in V>> toMap(destination: M) =
      use { channel.toMap(destination) }
  }

  companion object {
    /**
     * Connects to a Mysql or MariaDB database using the supplied credentials.
     * @param database the database name.
     * @param credentials the credentials to use for the connection (defaults to root unsecured credentials).
     * @param address the server address and port (localhost:3306 by default).
     * @param bufferSize the buffer size (4MB by default).
     */
    suspend fun to(
      database: String,
      credentials: MysqlAuthentication.Credentials = MysqlAuthentication.Credentials.UnsecuredCredentials(),
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 3306),
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
