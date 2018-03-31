package info.jdavid.mysql

import info.jdavid.sql.use
import kotlinx.coroutines.experimental.runBlocking
import org.junit.After
import org.junit.Test
import org.junit.Assert.*
import org.junit.Before
import java.util.regex.Pattern

class WorldTests {

  private val credentials: MysqlAuthentication.Credentials
  private val databaseName: String
  init {
    val properties = Utils.properties()
    val username = properties.getProperty("mysql_username") ?: "root"
    val password = properties.getProperty("mysql_password") ?: ""
    credentials = MysqlAuthentication.Credentials.PasswordCredentials(username, password)
    databaseName = properties.getProperty("mysql_database") ?: "mysql"
  }

  @Before
  fun createTables() {
    val sql = WorldTests::class.java.getResource("/world.sql").readText()
    val split = sql.split(Pattern.compile(";\\s*\\r?\\n"))
    runBlocking {
      credentials.connectTo(databaseName).use { connection ->
        connection.withTransaction {
          split.forEachIndexed { i: Int, it: String ->
            if (it.trim().isEmpty()) return@forEachIndexed
            connection.affectedRows(it)
          }
        }
      }
    }
  }

  @Test
  fun testData() {
    runBlocking {
      credentials.connectTo(databaseName).use { connection ->
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

  @After
  fun dropTables() {
    runBlocking {
      credentials.connectTo(databaseName).use { connection ->
        connection.affectedRows("DROP TABLE IF EXISTS countrylanguage,city,country")
      }
    }
  }

}
