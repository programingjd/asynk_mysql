package info.jdavid.asynk.mysql

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.nio.ByteBuffer
import java.nio.ByteOrder

class ProtocolTypesTests {

  @Test
  fun testThreeByteInteger() {
    sequenceOf(
      byteArrayOf(0, 0, 0, 0),
      byteArrayOf(1, 0, 0, 0),
      byteArrayOf(0xfa.toByte(), 0, 0, 0),
      byteArrayOf(0, 0xee.toByte(), 0, 0),
      byteArrayOf(1, 2, 3, 0),
      byteArrayOf(0xee.toByte(), 0xcc.toByte(), 0xaa.toByte(), 0)
    ).forEach {
      it.apply {
        assertEquals(intValue(this), BinaryFormat.threeByteInteger(ByteBuffer.wrap(this)))
      }
    }
  }

  @Suppress("UsePropertyAccessSyntax")
  private fun intValue(bytes: ByteArray) = ByteBuffer.wrap(bytes).
    order(ByteOrder.LITTLE_ENDIAN).getInt()

}
