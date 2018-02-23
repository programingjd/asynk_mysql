package info.jdavid.mysql

import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.Temporal
import java.util.Date

internal object BinaryFormat {

  @Suppress("UsePropertyAccessSyntax")
  fun parse(type: Byte, unsigned: Boolean, buffer: ByteBuffer): Any? {
    return when (type) {
      Types.BYTE -> buffer.get()
      Types.SHORT -> buffer.getShort()
      Types.INT, Types.INT24 -> buffer.getInt()
      Types.LONG -> buffer.getLong()
      Types.FLOAT -> buffer.getFloat()
      Types.DOUBLE -> buffer.getDouble()
      Types.DECIMAL -> getLengthEncodedString(buffer).toBigDecimal()
      Types.DATETIME, Types.TIMESTAMP -> {
        val n = buffer.get()
        assert(n == 7.toByte() || n == 11.toByte())
        val year = buffer.getShort().toInt()
        val month = buffer.get().toInt()
        val day = buffer.get().toInt()
        val hour = buffer.get().toInt()
        val min = buffer.get().toInt()
        val sec = buffer.get().toInt()
        val nano = if (n == 11.toByte()) buffer.getInt() * 1000000 else 0
        LocalDateTime.of(year, month, day, hour, min, sec, nano)
      }
      Types.DATE -> {
        val n = buffer.get()
        assert(n == 4.toByte())
        val year = buffer.getShort().toInt()
        val month = buffer.get().toInt()
        val day = buffer.get().toInt()
        date(LocalDate.of(year, month, day))
      }
      Types.VARCHAR, Types.VARSTRING, Types.STRING -> getLengthEncodedString(buffer)
      Types.BLOB, Types.TINYBLOB, Types.MEDIUMBLOB, Types.LONGBLOB -> getLengthEncodedBlob(buffer)
      else -> throw RuntimeException("Unsupported type: ${type}")
    }
  }

  private fun date(temporal: Temporal): Date {
    return Date.from(when (temporal) {
      is OffsetDateTime -> temporal.toInstant()
      is LocalDateTime -> temporal.toInstant(ZoneOffset.UTC)
      is LocalDate -> Instant.ofEpochSecond(temporal.atStartOfDay(ZoneOffset.UTC).toEpochSecond())
      else -> throw RuntimeException()
    })
  }

  fun threeByteInteger(buffer: ByteBuffer): Int {
    val one = buffer.get().toInt() and 0xff
    val two = buffer.get().toInt() and 0xff
    val three = buffer.get().toInt() and 0xff
    return one + two * 256 + three * 256 * 256
  }

  fun getLengthEncodedInteger(buffer: ByteBuffer): Long {
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

  fun getLengthEncodedBlob(buffer: ByteBuffer): ByteArray {
    val length = getLengthEncodedInteger(buffer).toInt()
    return ByteArray(length).apply { buffer.get(this) }
  }

  fun getLengthEncodedString(buffer: ByteBuffer) = String(getLengthEncodedBlob(buffer))

  fun getNullTerminatedString(buffer: ByteBuffer): String {
    val sb = StringBuilder(Math.min(255, buffer.remaining()))
    while (buffer.remaining() > 0) {
      val b = buffer.get()
      if (b == 0.toByte()) break
      sb.appendCodePoint(b.toInt())
    }
    return sb.toString()
  }

  fun getNullTerminatedString(buffer: ByteBuffer, end: Int): String {
    val sb = StringBuilder(Math.min(255, end - buffer.position()))
    while (buffer.position() < end) {
      val b = buffer.get()
      if (b == 0.toByte()) break
      sb.appendCodePoint(b.toInt())
    }
    return sb.toString()
  }

}
