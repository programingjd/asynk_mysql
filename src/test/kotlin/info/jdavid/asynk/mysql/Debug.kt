package info.jdavid.asynk.mysql

import info.jdavid.asynk.sql.*
import kotlinx.coroutines.Dispatchers
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
                (1..100).forEach {
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
                ).iterate {
                  it.next()
                }
                println(firstName)
                println("mapping")
                println(connection.entries<Int, String>(
                  """
                    SELECT id, name FROM test
                  """.trimIndent(),
                  "id", "name").toMap().
                  map { "${it.key} -> ${it.value}" }.joinToString("\n"))
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
