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

  @Test
  fun testMysql() {
    val address = InetSocketAddress("ensembldb.ensembl.org", 3306)
    val databaseName = "aedes_aegypti_core_48_1b"
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
    val databaseName = "compara_mart_homology_48"
    runBlocking {
      credentials.connectTo(databaseName, address, 16*1024).use {
        it.rows(
          """
            SELECT * FROM compara_mart_homology_48.compara_aaeg_aaeg_paralogs__paralogs__main LIMIT 1
          """.trimIndent()
        ).toList().apply {
          Assert.assertEquals(5, size)
        }
      }
    }
  }

}
