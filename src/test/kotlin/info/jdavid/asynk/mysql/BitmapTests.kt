package info.jdavid.asynk.mysql

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class BitmapTests {

  @Test
  fun testSize() {
    assertEquals(0, Bitmap(0).bytes.size)
    assertEquals(1, Bitmap(1).bytes.size)
    assertEquals(1, Bitmap(2).bytes.size)
    assertEquals(1, Bitmap(3).bytes.size)
    assertEquals(1, Bitmap(6).bytes.size)
    assertEquals(1, Bitmap(7).bytes.size)
    assertEquals(1, Bitmap(8).bytes.size)
    assertEquals(2, Bitmap(9).bytes.size)

    assertEquals(1, Bitmap(0, 2).bytes.size)
    assertEquals(1, Bitmap(1, 2).bytes.size)
    assertEquals(1, Bitmap(2, 2).bytes.size)
    assertEquals(1, Bitmap(6, 2).bytes.size)
    assertEquals(2, Bitmap(7, 2).bytes.size)
  }

  @Test
  fun testValuesSmallBitmap() {
    testValuesSmallBitmap(0)
    testValuesSmallBitmap(2)
  }

  @Test
  fun testValuesBitmap() {
    testValuesBitmap(0)
    testValuesBitmap(2)
  }

  private fun testValuesSmallBitmap(offset: Int) {
    Bitmap(3, offset).apply {
      assertFalse(get(0))
      assertFalse(get(1))
      assertFalse(get(2))
      set(0, true)
      assertTrue(get(0))
      assertFalse(get(1))
      assertFalse(get(2))
      set(1, true)
      assertTrue(get(0))
      assertTrue(get(1))
      assertFalse(get(2))
      set(2, true)
      assertTrue(get(0))
      assertTrue(get(1))
      assertTrue(get(2))
      set(1, false)
      assertTrue(get(0))
      assertFalse(get(1))
      assertTrue(get(2))
      set(0, false)
      assertFalse(get(0))
      assertFalse(get(1))
      assertTrue(get(2))
      set(2, true)
      assertFalse(get(0))
      assertFalse(get(1))
      assertTrue(get(2))
      set(1, true)
      assertFalse(get(0))
      assertTrue(get(1))
      assertTrue(get(2))
      set(1, true)
      assertFalse(get(0))
      assertTrue(get(1))
      assertTrue(get(2))
    }
  }

  private fun testValuesBitmap(offset: Int) {
    Bitmap(16, offset).apply {
      for (i in 0 until 16) {
        assertFalse(get(i))
      }
      set(2, true)
      for (i in 0 until 16) {
        assertEquals(i == 2, get(i))
      }
      set(2, true)
      for (i in 0 until 16) {
        assertEquals(i == 2, get(i))
      }
      set(5, true)
      for (i in 0 until 16) {
        assertEquals(i == 2 || i == 5, get(i))
      }
      set(5, false)
      for (i in 0 until 16) {
        assertEquals(i == 2, get(i))
      }
      set(3, false)
      for (i in 0 until 16) {
        assertEquals(i == 2, get(i))
      }
      set(14, true)
      for (i in 0 until 16) {
        assertEquals(i == 2 || i == 14, get(i))
      }
      set(15, true)
      for (i in 0 until 16) {
        assertEquals(i == 2 || i == 14 || i == 15, get(i))
      }
      set(15, true)
      for (i in 0 until 16) {
        assertEquals(i == 2 || i == 14 || i == 15, get(i))
      }
      set(15, false)
      for (i in 0 until 16) {
        assertEquals(i == 2 || i == 14, get(i))
      }
      set(2, false)
      for (i in 0 until 16) {
        assertEquals(i == 14, get(i))
      }
    }
  }

}
