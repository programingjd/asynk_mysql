package info.jdavid.asynk.mysql

import info.jdavid.asynk.sql.use
import kotlinx.coroutines.experimental.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.math.BigInteger
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class LocalDbTests {
  private val credentials: MysqlAuthentication.Credentials
  private val databaseName: String
  init {
    credentials = MysqlAuthentication.Credentials.PasswordCredentials("test", "asynk")
    databaseName = "world"
  }

  companion object {
    @JvmStatic @BeforeAll
    fun startDockerContainers() {
      Executors.newCachedThreadPool().apply {
        Docker.DatabaseVersion.values().forEach {
          submit {
            Docker.startContainer(it)
          }
        }
        shutdown()
      }.awaitTermination(300, TimeUnit.SECONDS)
    }
    @JvmStatic @AfterAll
    fun stopDockerContainers() {
      Executors.newCachedThreadPool().apply {
        Docker.DatabaseVersion.values().forEach {
          submit {
            Docker.stopContainer(it)
          }
        }
        shutdown()
      }.awaitTermination(15, TimeUnit.SECONDS)
    }
  }


  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testSimple(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              name           text      NOT NULL,
              bytes          blob      DEFAULT NULL,
              active         boolean   DEFAULT FALSE NOT NULL,
              creation_date  timestamp DEFAULT NOW()
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (name) VALUES (?)
          """.trimIndent(),
          listOf("Name1")
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (name, active) VALUES (?, ?)
          """.trimIndent(),
          listOf("Name2", true)
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (name, active) VALUES (?, ?)
          """.trimIndent(),
          listOf("Name3", false)
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY name
          """.trimIndent()
        ).toList().apply {
          assertEquals(3, size)
          assertEquals("Name1", get(0)["name"])
          assertFalse(get(0)["active"] as Boolean)
          assertEquals("Name2", get(1)["name"])
          assertTrue(get(1)["active"] as Boolean)
          assertEquals("Name3", get(2)["name"])
          assertFalse(get(2)["active"] as Boolean)
        }
        assertEquals(2, it.affectedRows(
          """
            UPDATE test SET active=TRUE WHERE active=FALSE
          """.trimIndent()
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY name
          """.trimIndent()
        ).toList().apply {
          assertEquals(3, size)
          assertEquals("Name1", get(0)["name"])
          assertTrue(get(0)["active"] as Boolean)
          assertEquals("Name2", get(1)["name"])
          assertTrue(get(1)["active"] as Boolean)
          assertEquals("Name3", get(2)["name"])
          assertTrue(get(2)["active"] as Boolean)
        }
        assertEquals(2, it.affectedRows(
          """
            UPDATE test SET active=FALSE WHERE NOT(name LIKE '%2')
          """.trimIndent()
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY name
          """.trimIndent()
        ).toList().apply {
          assertEquals(3, size)
          assertEquals("Name1", get(0)["name"])
          assertFalse(get(0)["active"] as Boolean)
          assertEquals("Name2", get(1)["name"])
          assertTrue(get(1)["active"] as Boolean)
          assertEquals("Name3", get(2)["name"])
          assertFalse(get(2)["active"] as Boolean)
        }
        assertEquals(2, it.affectedRows(
          """
              DELETE FROM test WHERE active=FALSE
            """
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY name
          """.trimIndent()
        ).toList().apply {
          assertEquals(1, size)
          assertEquals("Name2", get(0)["name"])
          assertTrue(get(0)["active"] as Boolean)
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testPreparedStatement(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              name           text      NOT NULL,
              active         boolean   DEFAULT FALSE NOT NULL,
              creation_date  timestamp DEFAULT NOW()
            )
          """.trimIndent()
        ))
        it.prepare("""
            INSERT INTO test (name) VALUES (?)
          """.trimIndent()).apply {
          assertFalse(temporary)
          assertEquals(1, affectedRows(listOf("Name1")))
          assertEquals(1, affectedRows(listOf("Name2")))
          assertEquals(1, affectedRows(listOf("Name3")))
          assertEquals(1, affectedRows(listOf("Name4")))
          aClose()
          try {
            affectedRows(listOf("Name5"))
            fail("Execution of closed prepared statement should have failed.")
          }
          catch (ignore: Exception) {}
        }
        it.prepare("SELECT * FROM test WHERE active=?").apply {
          assertFalse(temporary)
          assertEquals(4, rows(listOf(false)).toList().size)
          assertEquals(0, rows(listOf(true)).toList().size)
          assertEquals(1, it.affectedRows("DELETE FROM test WHERE name=?", listOf("Name4")))
          assertEquals(3, rows(listOf(false)).toList().size)
          assertEquals(1, it.affectedRows("UPDATE test SET active=TRUE WHERE name=?", listOf("Name1")))
          assertEquals(2, rows(listOf(false)).toList().size)
          aClose()
          try {
            rows(listOf(false))
            fail("Execution of closed prepared statement should have failed.")
          }
          catch (ignore: Exception) {}
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testNull(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              i1             integer,
              s1             text      DEFAULT NULL,
              b1             boolean
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (i1, s1, b1) VALUES (?, ?, ?)
          """.trimIndent(),
          listOf(1234, null, true)
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (i1, b1) VALUES (?, ?)
          """.trimIndent(),
          listOf(null, false)
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (i1, b1) VALUES (?, ?)
          """.trimIndent(),
          listOf(null, null)
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          assertEquals(3, size)
          assertEquals(1234, get(0)["i1"])
          assertNull(get(0)["s1"])
          assertEquals(true, get(0)["b1"])
          assertNull(get(1)["i1"])
          assertNull(get(1)["s1"])
          assertEquals(false,get(1)["b1"])
          assertNull(get(2)["i1"])
          assertNull(get(2)["s1"])
          assertNull(get(2)["b1"])
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testUnsigned(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              i1             integer   UNSIGNED NOT NULL DEFAULT 4294967295,
              i2             bigint    UNSIGNED NOT NULL
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (i1, i2) VALUES (?, ?)
          """.trimIndent(),
          listOf(2400000000L, BigInteger("10000000000000000000"))
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (i2) VALUES (?)
          """.trimIndent(),
          listOf(1)
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          assertEquals(2, size)
          assertEquals(2400000000L, get(0)["i1"])
          assertEquals(BigInteger("10000000000000000000"), get(0)["i2"])
          assertEquals(4294967295L, get(1)["i1"])
          assertEquals(BigInteger.valueOf(1L), get(1)["i2"])
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testBoolean(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              b1             bit       NOT NULL,
              b2             boolean   NOT NULL,
              b3             bit       DEFAULT NULL,
              b4             boolean   DEFAULT FALSE NOT NULL,
              b5             bit       DEFAULT TRUE NOT NULL
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (b1, b2, b3) VALUES (?, ?, ?)
          """.trimIndent(),
          listOf(true, true, true)
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (b1, b2, b3) VALUES (?, ?, ?)
          """.trimIndent(),
          listOf(false, false, false)
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (b1, b2, b3, b4, b5) VALUES (?, ?, ?, ?, ?)
          """.trimIndent(),
          listOf(true, true, true, true, true)
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          assertEquals(3, size)
          for (i in 1..5) {
            assertEquals(i != 4, get(0)["b${i}"], "b${i}")
          }
          for (i in 1..5) {
            assertEquals(i == 5, get(1)["b${i}"])
          }
          for (i in 1..5) {
            assertEquals(true, get(2)["b${i}"])
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testEnum(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              enum1          enum('a','b','c','d')
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (enum1) VALUES (?)
          """.trimIndent(),
          listOf("a")
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (enum1) VALUES (?)
          """.trimIndent(),
          listOf(null)
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          assertEquals(2, size)
          assertEquals("a", get(0)["enum1"])
          assertNull(get(1)["enum1"])
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testSet(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              set1           SET('a', 'b', 'c')
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (set1) VALUES (?)
          """.trimIndent(),
          listOf("a,b")
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (set1) VALUES (?)
          """.trimIndent(),
          listOf("c")
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (set1) VALUES (?)
          """.trimIndent(),
          listOf("")
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (set1) VALUES (?)
          """.trimIndent(),
          listOf(null)
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          assertEquals(4, size)
          assertEquals("a,b", get(0)["set1"])
          assertEquals("c", get(1)["set1"])
          assertEquals("", get(2)["set1"])
          assertNull(get(3)["set1"])
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testBlob(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val bytes = byteArrayOf(0x02.toByte(), 0x00.toByte(), 0xfa.toByte(), 0x8e.toByte())
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              data           BLOB
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (data) VALUES (?)
          """.trimIndent(),
          listOf(bytes)
        ))
        it.rows(
          """
            SELECT * FROM test
          """.trimIndent()
        ).toList().apply {
          assertEquals(1, size)
          assertTrue(get(0)["data"] is ByteArray)
          val data = get(0)["data"] as ByteArray
          assertArrayEquals(bytes, data)
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testSizes(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val sizes = listOf(
        ByteArray(50) { it.toByte() },
        ByteArray(300) { it.toByte() },
        ByteArray(15000) { it.toByte() },
        ByteArray(45000) { it.toByte() }//,
//        ByteArray(100000) { it.toByte() }
      )
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address, 16000000).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              data           LONGBLOB
            )
          """.trimIndent()
        ))
        sizes.forEach {bytes ->
          assertEquals(1, it.affectedRows(
            """
            INSERT INTO test (data) VALUES (?)
          """.trimIndent(),
            listOf(bytes)
          ))
        }
        it.rows(
          """
            SELECT * FROM test
          """.trimIndent()
        ).toList().apply {
          assertEquals(sizes.size, size)
          sizes.forEachIndexed { index, bytes ->
            val data = get(index)["data"]
            assertTrue(data is ByteArray)
            assertArrayEquals(bytes, data as ByteArray)
          }
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testTransactions(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              v              integer   NOT NULL DEFAULT 0
            )
          """.trimIndent()
        ))
        it.withTransaction {
          assertEquals(1, it.affectedRows(
            """
            INSERT INTO test (v) VALUES (?)
          """.trimIndent(),
            listOf(34)
          ))
          assertEquals(1, it.affectedRows(
            """
            INSERT INTO test (v) VALUES (?)
          """.trimIndent(),
            listOf(35)
          ))
        }
        try {
          it.withTransaction {
            assertEquals(1, it.affectedRows(
              """
            INSERT INTO test (v) VALUES (?)
          """.trimIndent(),
              listOf(52)
            ))
            throw RollbackException()
          }
        }
        catch (ignore: RollbackException) {}
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          assertEquals(2, size)
          assertEquals(34, get(0)["v"])
          assertEquals(35, get(1)["v"])
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testWorldData(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use { connection ->
        assertEquals(
          239L,
          connection.rows("SELECT count(*) AS count FROM country").
            toList().first()["count"]
        )
        val codes = connection.rows(
          "SELECT DISTINCT CountryCode AS code FROM city"
        ).toList().map { it["code"] }
        assertEquals(232, codes.size)
        assertTrue(codes.contains("FRA"))
        assertEquals(codes.toSet().size, codes.size)
        val all = connection.rows(
          "SELECT Code AS code FROM country"
        ).toList().map { it["code"] }.toSet()
        assertEquals(239, all.size)
        assertEquals(all.toSet().size, all.size)
        assertTrue(all.containsAll(codes))

        val cities = connection.rows(
          "SELECT Name AS city FROM city WHERE CountryCode=? ORDER BY Population DESC",
          listOf("FRA")
        ).toList().map { it["city"] }
        assertEquals(40, cities.size)
        assertEquals("Paris", cities[0])
        assertEquals("Marseille", cities[1])
        assertEquals("Lyon", cities[2])
      }
    }
  }

  private class RollbackException: RuntimeException()

}
