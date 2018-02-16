package info.jdavid.mysql

import java.nio.ByteBuffer
import java.util.*

sealed class Packet {

  internal interface FromServer
  internal interface FromClient {
    fun writeTo(buffer: ByteBuffer)
  }

  class OKPacket(private val info: String): FromServer, Packet() {
    override fun toString() = "OK(){\n${info}}"
  }

  class ErrPacket(private val errorCode: Short,
                  private val sqlState: String,
                  private val message: String): FromServer, Packet() {
    override fun toString() = "ERR(code: ${errorCode}, state: ${sqlState}){\n${message}}"
  }

  class EOFPacket: FromServer, Packet() {
    override fun toString() = "EOF()"
  }

  class HandshakePacket(private val capabilities: BitSet,
                        private val scramble: ByteArray,
                        private val auth: String?): FromServer, Packet() {
    override fun toString() = "HANDSHAKE(auth: ${auth ?: "null"})"
  }

  companion object {

    @Suppress("UsePropertyAccessSyntax")
    internal fun fromBytes(buffer: ByteBuffer): Packet.FromServer {
      val length = buffer.getInt()
//      val length = threeByteInteger(buffer)
//      val sequenceId = buffer.get()
//      assert(sequenceId == 0.toByte())
      if (length > buffer.remaining()) throw RuntimeException("Connection buffer too small.")
      val start = buffer.position()
      val first = buffer.get()
      when (first) {
        0x00.toByte() -> {
          val affectedRows = getLengthEncodedInteger(buffer)
          val lastInsertId = getLengthEncodedInteger(buffer)
          val status = BitSet.valueOf(ByteArray(2).apply { buffer.get(this) })
          val warningCount = buffer.getShort()
          val info = ByteArray(start + length - buffer.position()).let {
            buffer.get(it)
            String(it)
          }
          return OKPacket(info)
        }
        0xff.toByte() -> {
          val errorCode = buffer.getShort()
          buffer.get() // marker
          val sqlState = ByteArray(5).let {
            buffer.get(it)
            String(it)
          }
          val message = ByteArray(start + length - buffer.position()).let {
            buffer.get(it)
            String(it)
          }
          return ErrPacket(errorCode, sqlState, message)
        }
        0xfe.toByte() -> {
          val warningCount = buffer.getShort()
          val status = BitSet.valueOf(ByteArray(2).apply { buffer.get(this) })
          assert(start + length == buffer.position())
          return EOFPacket()
        }
        0x0a.toByte() -> {
          val serverVersion = getNullTerminatedString(buffer)
          val connectionId = buffer.getInt()
          var scramble = ByteArray(8).apply { buffer.get(this) }
          val filler1 = buffer.get()
          assert(filler1 == 0.toByte())
          val capabilitiesBytes = ByteArray(4).apply { buffer.get(this, 2, 2) }
          if (start + length > buffer.position()) {
            val collation = buffer.get()
            val status = BitSet.valueOf(ByteArray(2).apply { buffer.get(this) })
            buffer.get(capabilitiesBytes, 0, 2)
            val n = Math.max(13, buffer.get() - 8)
            buffer.get(ByteArray(10))
            scramble = ByteArray(scramble.size + n).apply {
              System.arraycopy(scramble, 0, this, 0, scramble.size)
              buffer.get(this, scramble.size, n)
            }
          }
          val auth = getNullTerminatedString(buffer, start + length)
          return HandshakePacket(BitSet.valueOf(capabilitiesBytes), scramble, auth)
        }
        else -> throw IllegalArgumentException(first.toString(16))
      }
    }

    private fun threeByteInteger(buffer: ByteBuffer): Int {
      return buffer.get().toInt() and 0xff +
        buffer.get().toInt() and 0xff shl  8 +
        buffer.get().toInt() and 0xff shl 16
    }

    private fun getLengthEncodedInteger(buffer: ByteBuffer): Long {
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

}
