package info.jdavid.asynk.mysql

import java.sql.DriverManager

class Debug {

  companion object {
    @JvmStatic
    fun main(args: Array<String>) {
      Docker.DatabaseVersion.values().last().let { version ->
        Docker.startContainer(version)
        try {
          val conn =
            DriverManager.getConnection("jdbc:mysql://localhost:${version.port}/world?useSSL=false&allowPublicKeyRetrieval=true", "test", "asynk")
          conn.close()
        }
        finally {
          Docker.stopContainer(version)
        }
      }
    }
  }

}
