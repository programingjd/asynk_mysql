package info.jdavid.asynk.mysql

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClients
import java.io.File
import java.util.*

object Utils {
  internal fun properties(): Properties {
    var dir: File? = File(this::class.java.protectionDomain.codeSource.location.toURI())
    while (dir != null) {
      if (File(dir, ".git").exists()) break
      dir = dir.parentFile
    }
    val props = Properties()
    if (dir != null) {
      val file = File(dir, "local.properties")
      if (file.exists()) {
        props.load(file.reader())
      }
    }
    return props
  }

  private fun mountPath(): String {
    var dir: File? = File(this::class.java.protectionDomain.codeSource.location.toURI())
    while (dir != null) {
      if (File(dir, ".git").exists()) break
      dir = dir.parentFile
    }
    dir = File(dir, "data")
    return dir.canonicalPath
  }

  private val dockerApiUrl = "http://localhost:2375"

  enum class MysqlVersion(val v: String) {
    FIFTY_FIVE("5.5"),
    FIFTY_SIX("5.6"),
    FIFTY_SEVEN("5.7"),
    EIGHTY("8.0")
  }

  internal fun startDocker(mysqlVersion: MysqlVersion = MysqlVersion.FIFTY_SEVEN) {
    HttpClients.createMinimal().let {
      try {
        it.execute(HttpGet("${dockerApiUrl}/version")).use {
          if (it.statusLine.statusCode != 200) {
            throw RuntimeException("Docker is unreachable.")
          }
        }
      }
      catch (e: Exception) {
        println(
          "Docker did not respond. Please make sure that docker is running and that the option to expose " +
            "the daemon on tcp without TLS is enabled in the settings."
        )
        e.printStackTrace()
      }

      it.execute(HttpPost(
        "${dockerApiUrl}/containers/create?name=async_postgres_mysql_${mysqlVersion.v}"
      ).apply {
        val body = mapOf(
          "Image" to "mysql/mysql-server:${mysqlVersion.v}",
          "Env" to listOf(
            "MYSQL_ROOT_PASSWORD=root",
            "MYSQL_DATABASE=world",
            "MYSQL_USER=mysql",
            "MYSQL_PASSWORD=mysql",
            "MYSQL_LOG_CONSOLE=true"
          ),
          "HostConfig" to mapOf(
            "Binds" to listOf("${mountPath()}:/docker-entrypoint-initdb.d"),
            "PortBindings" to mapOf(
              "3306/tcp" to listOf(
                mapOf("HostPort" to "3306")
              )
            )
          ),
          "ExposedPorts" to mapOf("3306/tcp" to emptyMap<String,Any>())
        )
        entity = ByteArrayEntity(ObjectMapper().writeValueAsBytes(body), ContentType.APPLICATION_JSON)
      }).use {
        println(String(it.entity.content.readAllBytes()))
      }

      it.execute(HttpPost(
        "${dockerApiUrl}/containers/async_postgres_mysql_${mysqlVersion.v}/start"
      )).use {
        if (it.statusLine.statusCode != 204 && it.statusLine.statusCode != 304)
          throw RuntimeException(String(it.entity.content.readAllBytes()))
      }
    }

  }

  internal fun stopDocker(mysqlVersion: MysqlVersion = MysqlVersion.FIFTY_SEVEN) {
    HttpClients.createMinimal().let {
      it.execute(HttpPost(
        "${dockerApiUrl}/containers/async_postgres_mysql_${mysqlVersion.v}/stop"
      )).use {
        if (it.statusLine.statusCode != 204 && it.statusLine.statusCode != 304)
          throw RuntimeException(String(it.entity.content.readAllBytes()))
      }
    }
    HttpClients.createMinimal().let {
      it.execute(HttpDelete(
        "${dockerApiUrl}/containers/async_postgres_mysql_${mysqlVersion.v}/stop"
      )).use {
        if (it.statusLine.statusCode != 204 && it.statusLine.statusCode != 404)
          throw RuntimeException(String(it.entity.content.readAllBytes()))
      }
    }
  }

  @JvmStatic
  fun main(args: Array<String>) {
    //println(mountPath())
    startDocker()
  }

}
