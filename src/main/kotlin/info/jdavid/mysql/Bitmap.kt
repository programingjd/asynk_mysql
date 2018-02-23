package info.jdavid.mysql

import java.nio.ByteBuffer

internal class Bitmap(n: Int, private val offset: Int = 0) {
  internal val bytes = ByteArray((n + 7 + offset) / 8)

  fun set(buffer: ByteBuffer): Bitmap {
    buffer.get(bytes)
    return this
  }

  fun get(index: Int): Boolean {
    val i = (index + offset) / 8
    val j = (index + offset) % 8
    return bytes[i].toInt() shr j and 1 == 1
  }

  fun set(index: Int, value: Boolean) {
    val i = (index + offset) / 8
    val j = (index + offset) % 8
    if (value) {
      bytes[i] = (bytes[i].toInt() or (1 shl j)).toByte()
    }
    else {
      bytes[i] = (bytes[i].toInt() and (1 shl j).inv()).toByte()
    }
  }

}
