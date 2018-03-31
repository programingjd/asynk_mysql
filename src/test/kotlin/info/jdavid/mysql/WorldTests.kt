package info.jdavid.mysql

import info.jdavid.sql.use
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test
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

  @Test
  fun testImport() {
    val sql = WorldTests::class.java.getResource("/world.sql").readText()
    val split = sql.split(Pattern.compile(";\\s*\\r?\\n"))
    runBlocking {
      credentials.connectTo(databaseName).use { connection ->
        split.forEach {
          connection.affectedRows("${it};")
        }
      }
    }
  }

}
