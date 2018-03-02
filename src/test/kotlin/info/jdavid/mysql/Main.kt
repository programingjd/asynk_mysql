package info.jdavid.mysql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import info.jdavid.sql.use
import kotlinx.coroutines.experimental.runBlocking

fun json(any: Any?) = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(any)

fun main(args: Array<String>) {
  val properties = Utils.properties()
  val username = properties.getProperty("mysql_username") ?: "root"
  val password = properties.getProperty("mysql_password") ?: ""
  val credentials = MysqlAuthentication.Credentials.PasswordCredentials(username, password)
  val database = properties.getProperty("mysql_database") ?: "mysql"
  runBlocking {
    credentials.connectTo(database).use {
//      val preparedStatement = it.prepare("""
//          SELECT * FROM demo WHERE 1
//        """.trimIndent())
//      println(preparedStatement.rows().toList())
//      val inserted = it.affectedRows("INSERT INTO demo (name, flag, time) VALUES(?, ?, NOW())",
//                                     listOf("TestInsert", false))
//      println(inserted)
      println(it.rows("SELECT * FROM demo WHERE flag=?", listOf(true)).toList().joinToString("\n"))
    }
  }
}

