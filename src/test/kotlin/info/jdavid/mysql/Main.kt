package info.jdavid.mysql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import kotlinx.coroutines.experimental.runBlocking

fun json(any: Any?) = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(any)

fun main(args: Array<String>) {
  val username = "root"
  val password = "root"
  val database = "test"
  runBlocking {
    MysqlAuthentication.MysqlCredentials.PasswordCredentials(username, password).
      connectTo(database).use {
        val preparedStatement = it.prepare("""
            SELECT * FROM demo WHERE 1
          """.trimIndent())
        println(preparedStatement.rows().toList())
    }
  }
}

