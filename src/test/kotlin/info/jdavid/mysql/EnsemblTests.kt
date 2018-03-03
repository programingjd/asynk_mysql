package info.jdavid.mysql

import info.jdavid.sql.use
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Assert
import org.junit.Test
import java.net.InetSocketAddress

// Ensembl public SQL Servers
// https;//www.ensembl.org
// https://www.ensembl.org/info/data/mysql.html
class EnsemblTests {

  private val credentials =
    MysqlAuthentication.Credentials.PasswordCredentials("anonymous", "")
  private val databaseName = "aedes_aegypti_core_48_1b"

  @Test
  fun testMysql() {
    val address = InetSocketAddress("ensembldb.ensembl.org", 3306)
    runBlocking {
      credentials.connectTo(databaseName, address).use {
        it.rows(
          """
            SELECT gene_id FROM gene ORDER BY gene_id LIMIT 1
          """.trimIndent()
        ).toList().apply {
          Assert.assertEquals(1, size)
        }
      }
    }
  }

  @Test
  fun testMariadb() {
    val address = InetSocketAddress("martdb.ensembl.org", 5316)
    runBlocking {
      credentials.connectTo(databaseName, address).use {
        it.rows(
          """
            SELECT * FROM gene ORDER BY id LIMIT 5
          """.trimIndent()
        ).toList().apply {
          Assert.assertEquals(5, size)
        }
      }
    }
  }

}
