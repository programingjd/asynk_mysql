package info.jdavid.asynk.mysql

import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal
import java.util.BitSet
import java.util.Date

internal object BinaryFormat {

  fun write(value: Any, type: Byte, length: Long, unsigned: Boolean, buffer: ByteBuffer) {
    when (type) {
      Types.BIT -> {
        if (length == 1L) {
          buffer.put(0x01.toByte())
          buffer.put(when(value) {
            is Boolean -> if (value == false) 0x00.toByte() else 0x01.toByte()
            is CharSequence -> when(value.toString().toLowerCase()) {
              "0", "false", "f", "n", "no" -> 0x00.toByte()
              "1", "true", "t", "y", "yes" -> 0x01.toByte()
              else -> throw IllegalArgumentException()
            }
            is Number -> value.toByte()
            is ByteArray -> if (length != 1L) throw IllegalArgumentException() else value[0]
            is BitSet -> if (value.size() > 1) throw IllegalArgumentException() else {
              if (!value.get(0)) 0x00.toByte() else 0x01.toByte()
            }
            is Bitmap -> if (value.bytes.size - value.offset != 1) {
              throw IllegalArgumentException()
            } else value.bytes[value.offset]
            else -> throw IllegalArgumentException()
          })
        }
        else {
          val n: Int = length.toInt() + 7 / 8
          buffer.put(n.toByte())
          buffer.put(when(value) {
            is BitSet -> {
              if (value.size() > n) throw IllegalArgumentException()
              val bytes = value.toByteArray()
              if (bytes.size == n) bytes else {
                ByteArray(n).apply { System.arraycopy(bytes, 0, this, 0, bytes.size) }
              }
            }
            is Bitmap -> {
              if (value.bytes.size - value.offset != n) throw IllegalArgumentException()
              if (value.offset == 0) value.bytes else value.bytes.copyOfRange(value.offset, value.bytes.size)
            }
            is ByteArray -> {
              if (value.size != n) throw IllegalArgumentException()
              value
            }
            else -> throw IllegalArgumentException()
          })
        }
      }
      Types.BYTE -> {
        buffer.put(
          if (value is Boolean) {
            if (value) 0x01.toByte() else 0x00.toByte()
          }
          else {
            if (value !is Number) throw IllegalArgumentException()
            if (unsigned) (value.toInt() and 0xff).toByte() else value.toByte()
          }
        )
      }
      Types.SHORT -> {
        if (value !is Number) throw IllegalArgumentException()
        buffer.putShort(if (unsigned) (value.toInt() and 0xffff).toShort() else value.toShort())
      }
      Types.INT, Types.INT24 -> {
        if (value !is Number) throw IllegalArgumentException()
        buffer.putInt(if (unsigned) (value.toLong() and 0xffffffffL).toInt() else value.toInt())
      }
      Types.LONG -> {
        if (value !is Number) throw IllegalArgumentException()
        buffer.putLong(value.toLong())
      }
      Types.FLOAT -> {
        if (value !is Number) throw IllegalArgumentException()
        buffer.putFloat(value.toFloat())
      }
      Types.DOUBLE -> {
        if (value !is Number) throw IllegalArgumentException()
        buffer.putDouble(value.toDouble())
      }
      Types.NUMERIC, Types.DECIMAL -> {
        if (value !is Number) throw IllegalArgumentException()
        val bytes = value.toString().toByteArray(Charsets.US_ASCII)
        setLengthEncodedInteger(bytes.size, buffer)
        buffer.put(bytes)
      }
      Types.DATETIME, Types.TIMESTAMP -> {
        val date = when(value) {
          is Date -> value.toInstant().atZone(ZoneOffset.UTC)
          is Temporal -> utcDateTime(value)
          else -> throw IllegalArgumentException()
        }
        buffer.put(0x07.toByte())
        buffer.putShort(date.year.toShort())
        buffer.put(date.month.value.toByte())
        buffer.put(date.dayOfMonth.toByte())
        buffer.put(date.hour.toByte())
        buffer.put(date.minute.toByte())
        buffer.put(date.second.toByte())
      }
      Types.DATETIME2, Types.TIMESTAMP2 -> {
        val date = when(value) {
          is Date -> value.toInstant().atZone(ZoneOffset.UTC)
          is Temporal -> utcDateTime(value)
          else -> throw IllegalArgumentException()
        }
        buffer.put(0x0b.toByte())
        buffer.putShort(date.year.toShort())
        buffer.put(date.month.value.toByte())
        buffer.put(date.dayOfMonth.toByte())
        buffer.put(date.hour.toByte())
        buffer.put(date.minute.toByte())
        buffer.put(date.second.toByte())
        buffer.putInt(date.nano / 1000)
      }
      Types.DATE -> {
        val date = when(value) {
          is Date -> value.toInstant().atZone(ZoneOffset.UTC)
          is Temporal -> utcDateTime(value)
          else -> throw IllegalArgumentException()
        }
        buffer.put(0x04.toByte())
        buffer.putShort(date.year.toShort())
        buffer.put(date.month.value.toByte())
        buffer.put(date.dayOfMonth.toByte())
      }
      Types.VARCHAR, Types.VARSTRING, Types.STRING -> {
        val bytes = if (value is ByteArray) value else when(value) {
          is CharSequence -> value.toString()
          is Boolean -> if (value) "1" else "0"
          is Number -> value.toString()
          is Temporal -> utcDateTime(value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          is Date -> value.toInstant().atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          else -> throw IllegalArgumentException()
        }.toByteArray(Charsets.UTF_8)
        setLengthEncodedInteger(bytes.size, buffer)
        buffer.put(bytes)
      }
      Types.ENUM, Types.SET, Types.BLOB, Types.TINYBLOB, Types.MEDIUMBLOB, Types.LONGBLOB -> {
        val bytes = when(value) {
          is CharSequence -> value.toString().toByteArray(Charsets.UTF_8)
          is ByteArray -> value
          else -> throw IllegalArgumentException()
        }
        setLengthEncodedInteger(bytes.size, buffer)
        buffer.put(bytes)
      }
      else -> throw RuntimeException("Unsupported type: ${type}")
    }
  }

  @Suppress("UsePropertyAccessSyntax")
  fun read(type: Byte, length: Long, unsigned: Boolean, binary: Boolean, buffer: ByteBuffer): Any? {
    return when (type) {
      Types.BIT -> {
        val n = buffer.get().toInt()
        if (length == 1L && n == 1) buffer.get() != 0.toByte() else Bitmap(length.toInt()).set(buffer,n).bytes
      }
      Types.BYTE -> when {
        unsigned -> (buffer.get().toInt() and 0xff).toShort()
        length == 1L -> buffer.get() != 0.toByte()
        else -> buffer.get()
      }
      Types.SHORT -> if (unsigned) buffer.getShort().toInt() and 0xffff else buffer.getShort()
      Types.INT, Types.INT24 -> if (unsigned) buffer.getInt().toLong() and 0xffffffffL else buffer.getInt()
      Types.LONG -> if (unsigned) {
        val value = buffer.getLong()
        if (value >= 0L) BigInteger.valueOf(value)
        else {
          val upper = value.ushr(32) and 0xffffffffL
          val lower = value.toInt().toLong() and 0xffffffffL
          (BigInteger.valueOf(upper) shl 32).plus(BigInteger.valueOf(lower))
        }
      } else buffer.getLong()
      Types.FLOAT -> buffer.getFloat()
      Types.DOUBLE -> buffer.getDouble()
      Types.NUMERIC, Types.DECIMAL -> getLengthEncodedString(buffer).toBigDecimal()
      Types.DATETIME, Types.DATETIME2, Types.TIMESTAMP, Types.TIMESTAMP2 -> {
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
      Types.VARCHAR, Types.VARSTRING, Types.STRING, Types.ENUM, Types.SET -> getLengthEncodedString(buffer)
      Types.BLOB, Types.TINYBLOB, Types.MEDIUMBLOB, Types.LONGBLOB -> {
        if (binary) getLengthEncodedBlob(buffer) else getLengthEncodedString(buffer)
      }
      else -> throw RuntimeException("Unsupported type: ${type}")
    }
  }

  private fun date(temporal: Temporal): Date {
    return Date.from(when (temporal) {
      is ZonedDateTime -> temporal.toInstant()
      is OffsetDateTime -> temporal.toInstant()
      is LocalDateTime -> temporal.toInstant(ZoneOffset.UTC)
      is LocalDate -> Instant.ofEpochSecond(temporal.atStartOfDay(ZoneOffset.UTC).toEpochSecond())
      else -> throw RuntimeException()
    })
  }

  private fun utcDateTime(temporal: Temporal): ZonedDateTime {
    return when (temporal) {
      is ZonedDateTime -> temporal
      is OffsetDateTime -> temporal.atZoneSameInstant(ZoneOffset.UTC)
      is LocalDateTime -> temporal.toInstant(ZoneOffset.UTC).atZone(ZoneOffset.UTC)
      is LocalDate -> temporal.atStartOfDay(ZoneOffset.UTC)
      is Instant -> temporal.atZone(ZoneOffset.UTC)
      else -> throw RuntimeException()
    }
  }

  fun threeByteInteger(buffer: ByteBuffer): Int {
    val one = buffer.get().toInt() and 0xff
    val two = buffer.get().toInt() and 0xff
    val three = buffer.get().toInt() and 0xff
    return one + two * 256 + three * 256 * 256
  }

  fun twoByteInteger(buffer: ByteBuffer): Int {
    val one = buffer.get().toInt() and 0xff
    val two = buffer.get().toInt() and 0xff
    return one + two * 256
  }

  fun getLengthEncodedInteger(buffer: ByteBuffer): Long {
    val first = buffer.get().toInt() and 0xff
    @Suppress("UsePropertyAccessSyntax")
    return when (first) {
      0xff -> throw RuntimeException()
      0xfe -> buffer.getLong()
      0xfd -> threeByteInteger(buffer).toLong()
      0xfc -> twoByteInteger(buffer).toLong()
      else -> first.toLong()
    }
  }

  fun setLengthEncodedInteger(value: Int, buffer: ByteBuffer) {
    when {
      value < 251 -> buffer.put(value.toByte())
      value < 65536 -> {
        buffer.put(0xfc.toByte())
        buffer.putShort((value and 0xffff).toShort())
      }
      value < 16777216 -> {
        buffer.putInt(value)
        buffer.put(buffer.position() - 4, 0xfd.toByte())
      }
      else -> {
        buffer.put(0xfe.toByte())
        buffer.putLong(value.toLong())
      }
    }
  }

  fun getLengthEncodedBlob(buffer: ByteBuffer): ByteArray {
    val length = getLengthEncodedInteger(buffer).toInt()
    return ByteArray(length).apply { buffer.get(this) }
  }

  fun getLengthEncodedString(buffer: ByteBuffer) = String(getLengthEncodedBlob(buffer))

  fun getNullTerminatedString(buffer: ByteBuffer): String {
    val buf = ByteBuffer.allocate(buffer.remaining())
    while (buffer.remaining() > 0) {
      val b = buffer.get()
      if (b == 0.toByte()) break
      buf.put(b)
    }
    buf.flip()
    return String(ByteArray(buf.remaining()).apply {
      buf.get(this)
      buf.clear()
    })
  }

  fun getNullTerminatedString(buffer: ByteBuffer, end: Int): String {
    val buf = ByteBuffer.allocate(end - buffer.position())
    while (buffer.position() < end) {
      val b = buffer.get()
      if (b == 0.toByte()) break
      buf.put(b)
    }
    buf.flip()
    return String(ByteArray(buf.remaining()).apply {
      buf.get(this)
      buf.clear()
    })
  }

}
