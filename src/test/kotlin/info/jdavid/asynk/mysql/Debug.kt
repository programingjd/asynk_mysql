package info.jdavid.asynk.mysql

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.net.InetAddress
import java.net.InetSocketAddress
//import java.sql.DriverManager

class Debug {

  companion object {
    @JvmStatic
    fun main(args: Array<String>) {
      Docker.DatabaseVersion.values().last().let { version ->
        Docker.startContainer(version)
        try {
//          val conn =
//            DriverManager.getConnection("jdbc:mysql://localhost:${version.port}/world?useSSL=false&allowPublicKeyRetrieval=true", "test", "asynk")
//          conn.close()
          val credentials = MysqlAuthentication.Credentials.PasswordCredentials("test", "asynk")
          val databaseName = "world"
          val address = InetSocketAddress(InetAddress.getLocalHost(), version.port)
          runBlocking {
            launch(Dispatchers.IO) {
              credentials.connectTo(databaseName, address).use { connection ->
                println("creating table")
                connection.affectedRows(
                  """
                    CREATE TEMPORARY TABLE test (
                      id             serial    PRIMARY KEY,
                      name           text      NOT NULL,
                      bytes          blob      DEFAULT NULL,
                      active         boolean   DEFAULT FALSE NOT NULL,
                      creation_date  timestamp DEFAULT NOW()
                    )
                  """.trimIndent()
                )
                println("adding rows")
                (1..1000).forEach {
                  connection.affectedRows(
                    """
                    INSERT INTO test (name) VALUES (?)
                  """.trimIndent(),
                    listOf("Name${it}")
                  )
                }
                println("selecting")
                val firstName = connection.values<String>(
                  """
                    SELECT name FROM test
                  """.trimIndent(),
                  "name"
                ).use {
                  it.iterator().next()
                }
                println(firstName)
                delay(1000)
              }
            }.join()
          }
        }
        finally {
          Docker.stopContainer(version)
        }
      }
    }
  }

}
