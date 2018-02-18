package info.jdavid.mysql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import kotlinx.coroutines.experimental.runBlocking

fun json(any: Any?) = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(any)

fun main(args: Array<String>) {
  val username = "root"
  val password = "root"
  val database = "mysql"
  runBlocking {
    Authentication.Credentials.PasswordCredentials(username, password).
      connectTo(database).use {
        val preparedStatement = it.prepare("""
            CREATE TEMPORARY TABLE test(
              id INT NOT NULL AUTO_INCREMENT,
              name VARCHAR(255) NOT NULL,
              PRIMARY KEY (id)
            )
          """.trimIndent())
    }
  }
}

