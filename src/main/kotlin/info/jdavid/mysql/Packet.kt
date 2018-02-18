package info.jdavid.mysql

import java.nio.ByteBuffer

sealed class Packet {

  internal interface FromServer
  internal interface FromClient {
    fun writeTo(buffer: ByteBuffer)
  }

  class HandshakeResponse(private val database: String,
                          private val username: String, private val authResponse: ByteArray,
                          private val handshake: Handshake): FromClient, Packet() {

    override fun writeTo(buffer: ByteBuffer) {
      val start = buffer.position()
      buffer.putInt(0)
      buffer.putInt(Capabilities.clientCapabilities())
      buffer.putInt(4092)
      buffer.put(Collations.UTF8)
      buffer.put(ByteArray(23))
      buffer.put(username.toByteArray())
      buffer.put(0.toByte())
      buffer.put(authResponse.size.toByte())
      buffer.put(authResponse)
      buffer.put(database.toByteArray())
      buffer.put(0.toByte())
      buffer.put(handshake.auth?.toByteArray())
      buffer.put(0.toByte())
      buffer.putInt(start, buffer.position() - start - 4)
      buffer.put(start + 3, 1.toByte())
    }
  }

  class StatementPrepare(private val query: String): FromClient, Packet() {
    override fun toString() = "StatementPrepare(): ${query}"
    override fun writeTo(buffer: ByteBuffer) {
      val start = buffer.position()
      buffer.putInt(0)
      buffer.put(22.toByte())
      buffer.put(query.toByteArray())
      buffer.putInt(start, buffer.position() - start - 4)
    }
  }


  //-----------------------------------------------------------------------------------------------


  class OK(internal val sequenceId: Byte, private val info: String): FromServer, Packet() {
    override fun toString() = "GenericOK(){\n${info}\n}"
  }

  class StatementPrepareOK(internal val sequenceId: Byte,
                           internal val statementId: Int,
                           internal val columnCount: Short,
                           internal val paramCount: Short): FromServer, Packet() {
    override fun toString() = "StatementPrepareOK(${statementId})"
  }

  class ColumnDefinition(private val name: String, private val table: String): FromServer, Packet() {
    override fun toString() = "ColumnDefinition(${table}.${name})"
  }

  class EOF : FromServer, Packet() {
    override fun toString() = "EOF()"
  }

  class Handshake(internal val connectionId: Int,
                  internal val scramble: ByteArray,
                  internal val auth: String?): FromServer, Packet() {
    override fun toString() = "HANDSHAKE(auth: ${auth ?: "null"})"
  }

  companion object {

    @Suppress("UsePropertyAccessSyntax", "UNCHECKED_CAST")
    internal fun <T: FromServer> fromBytes(buffer: ByteBuffer, expected: Class<T>?): T {
      val length = threeByteInteger(buffer)
      val sequenceId = buffer.get()
      if (length > buffer.remaining()) throw RuntimeException("Connection buffer too small.")
      val start = buffer.position()
      val first = buffer.get()
      if (first == 0xff.toByte()) {
        val errorCode = buffer.getShort()
        val sqlState = ByteArray(5).let {
          buffer.get(it)
          String(it)
        }
        val message = ByteArray(start + length - buffer.position()).let {
          buffer.get(it)
          String(it)
        }
        assert(start + length == buffer.position())
        throw Exception("Error code: ${errorCode}, SQLState: ${sqlState}\n${message}")
      }
      return when (expected) {
        OK::class.java -> {
          println("OK")
          assert(first == 0x00.toByte() || first == 0xfe.toByte())
          /*val affectedRows =*/ getLengthEncodedInteger(buffer)
          /*val lastInsertId =*/ getLengthEncodedInteger(buffer)
          /*val status =*/ ByteArray(2).apply { buffer.get(this) }
          /*val warningCount =*/ buffer.getShort()
          val info = ByteArray(start + length - buffer.position()).let {
            buffer.get(it)
            String(it)
          }
          assert(start + length == buffer.position())
          OK(sequenceId, info) as T
        }
        EOF::class.java -> {
          println("EOF")
          assert(first == 0xfe.toByte())
          // CLIENT_DEPRECATE_EOF is set -> EOF is replaced with OK
          /* val warningCount =*/ buffer.getShort()
          /*val status =*/ ByteArray(2).apply { buffer.get(this) }
          assert(start + length == buffer.position())
          return EOF() as T
        }
        StatementPrepareOK::class.java -> {
          println("STMT_PREPARE_OK")
          assert(first == 0x00.toByte())
          val statementId = buffer.getInt()
          val columnCount = buffer.getShort()
          val paramCount = buffer.getShort()
          /*val filler =*/ buffer.get()
          /*val warningCount =*/ buffer.getShort()
          assert(start + length == buffer.position())
          StatementPrepareOK(sequenceId, statementId, columnCount, paramCount) as T
        }
        ColumnDefinition::class.java -> {
          println("COLUMN_DEFINITION")
          assert(first == 0x00.toByte())
          val catalog = getLengthEncodedString(buffer)
          val schema = getLengthEncodedString(buffer)
          val table = getLengthEncodedString(buffer)
          val tableOrg = getLengthEncodedString(buffer)
          val name = getLengthEncodedString(buffer)
          val nameOrg = getLengthEncodedString(buffer)
          val n = getLengthEncodedInteger(buffer)
          val collation = buffer.getShort()
          val columnLength = buffer.getInt()
          val type = buffer.get()
          val flags = ByteArray(2).apply { buffer.get(this) }
          val maxDigits = buffer.get()
          val filler = ByteArray(2).apply { buffer.get(this) }
          assert(start + length == buffer.position())
          return ColumnDefinition(name, table) as T
        }
        Handshake::class.java -> {
          println("HANDSHAKE")
          assert(first == 0x0a.toByte())
          /*val serverVersion =*/ getNullTerminatedString(buffer)
          val connectionId = buffer.getInt()
          var scramble = ByteArray(8).apply { buffer.get(this) }
          val filler1 = buffer.get()
          assert(filler1 == 0.toByte())
          val capabilitiesBytes = ByteArray(4).apply { buffer.get(this, 2, 2) }
          if (start + length > buffer.position()) {
            /*val collation =*/ buffer.get()
            /*val status =*/ ByteArray(2).apply { buffer.get(this) }
            buffer.get(capabilitiesBytes, 0, 2)
            val n = Math.max(12, buffer.get() - 9)
            buffer.get(ByteArray(10))
            scramble = ByteArray(scramble.size + n).apply {
              System.arraycopy(scramble, 0, this, 0, scramble.size)
              buffer.get(this, scramble.size, n)
            }
            /*val filler2 =*/ buffer.get()
            //assert(filler2 == 0.toByte())
          }
          val auth = getNullTerminatedString(buffer, start + length)
          assert(start + length == buffer.position())
          return Handshake(connectionId, scramble, auth) as T
        }
        else -> throw IllegalArgumentException()
      }
    }

    internal fun threeByteInteger(buffer: ByteBuffer): Int {
      val one = buffer.get().toInt() and 0xff
      val two = buffer.get().toInt() and 0xff
      val three = buffer.get().toInt() and 0xff
      return one + two * 256 + three * 256 * 256
    }

    internal fun getLengthEncodedInteger(buffer: ByteBuffer): Long {
      val first = buffer.get()
      @Suppress("UsePropertyAccessSyntax")
      return when (first) {
        0xff.toByte() -> throw RuntimeException()
        0xfe.toByte() -> buffer.getLong()
        0xfd.toByte() -> threeByteInteger(buffer).toLong()
        0xfc.toByte() -> buffer.getInt().toLong()
        else -> first.toLong()
      }
    }

    private fun getLengthEncodedString(buffer: ByteBuffer): String {
      val length = getLengthEncodedInteger(buffer).toInt()
      return ByteArray(length).let {
        buffer.get(it)
        String(it)
      }
    }

    private fun getNullTerminatedString(buffer: ByteBuffer): String {
      val sb = StringBuilder(Math.min(255, buffer.remaining()))
      while (buffer.remaining() > 0) {
        val b = buffer.get()
        if (b == 0.toByte()) break
        sb.appendCodePoint(b.toInt())
      }
      return sb.toString()
    }

    private fun getNullTerminatedString(buffer: ByteBuffer, end: Int): String {
      val sb = StringBuilder(Math.min(255, end - buffer.position()))
      while (buffer.position() < end) {
        val b = buffer.get()
        if (b == 0.toByte()) break
        sb.appendCodePoint(b.toInt())
      }
      return sb.toString()
    }

  }

  class Exception(message: String): RuntimeException(message)

}
