package info.jdavid.mysql

import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test
import org.junit.Assert.*

class LocalDbTests {
  private val credentials: Authentication.Credentials
  private val databaseName: String
  init {
    val properties = Utils.properties()
    val username = properties.getProperty("mysql_username") ?: "root"
    val password = properties.getProperty("mysql_password") ?: ""
    credentials = Authentication.Credentials.PasswordCredentials(username, password)
    databaseName = properties.getProperty("mysql_database") ?: "mysql"
  }

  @Test
  fun testSimple() {
    runBlocking {
      credentials.connectTo(databaseName).use {
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

  @Test
  fun testPreparedStatement() {
    runBlocking {
      credentials.connectTo(databaseName).use {
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
          close()
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
          close()
          try {
            rows(listOf(false))
            fail("Execution of closed prepared statement should have failed.")
          }
          catch (ignore: Exception) {}
        }
      }
    }
  }

}
