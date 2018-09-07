package info.jdavid.asynk.mysql

import info.jdavid.asynk.sql.use
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.runBlocking
import java.net.InetAddress
import java.net.InetSocketAddress
import java.sql.DriverManager

class Debug {

  companion object {
    @JvmStatic
    fun main(args: Array<String>) {
//      Docker.DatabaseVersion.values().last().let { version ->
//        Docker.startContainer(version)
//        try {
//          val conn =
//            DriverManager.getConnection("jdbc:mysql://localhost:${version.port}/world?useSSL=false&allowPublicKeyRetrieval=true", "test", "asynk")
//          conn.close()
//        }
//        finally {
//          Docker.stopContainer(version)
//        }
//      }
      val credentials =
        MysqlAuthentication.Credentials.PasswordCredentials("alexis", "password")
      val serverAddress = InetAddress.getByName("35.205.205.139")
      runBlocking(CommonPool) {
        credentials.connectTo("mobile_app", serverAddress).use { connection ->
          // returns a list of one row as a map of key (column name or alias) to value.
          connection.rows("SELECT * from Sounds_byte").toList()

        }

      }
    }
  }

}
