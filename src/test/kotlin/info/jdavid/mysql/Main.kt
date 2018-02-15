package info.jdavid.mysql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import kotlinx.coroutines.experimental.runBlocking

fun json(any: Any?) = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(any)

fun main(args: Array<String>) {
  val username = "mysql"
  val password = "mysql"
  val database = "mysql"
  runBlocking {
    Authentication.Credentials.PasswordCredentials(username, password).
      connectTo(database).use {

    }
  }
}

