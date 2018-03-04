package info.jdavid.mysql

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.security.MessageDigest
typealias MysqlCredentials=info.jdavid.sql.Credentials<MysqlConnection>

object MysqlAuthentication {

  internal suspend fun authenticate(connection: MysqlConnection,
                                    database: String,
                                    credentials: MysqlCredentials) {
    val handshake = connection.receive(Packet.Handshake::class.java)
    when (handshake.auth) {
      null -> throw RuntimeException("Unsupported authentication method: null")
      "mysql_old_password", "mysql_clear_password", "dialog" -> throw Exception(
        "Unsupported authentication method: ${handshake.auth}"
      )
      "auth_gssapi_client" -> throw Exception("Incompatible credentials.")
      "mysql_native_password" -> {
        if (credentials !is Credentials.PasswordCredentials) throw Exception("Incompatible credentials.")
        val authResponse = nativePassword(credentials.password, handshake.scramble)
        connection.send(Packet.HandshakeResponse(database, credentials.username, authResponse, handshake))
        val switch = connection.receive(Packet.AuthSwitchRequest::class.java)
        assert(switch.sequenceId == 2.toByte())
        if (switch.auth != null) {
          throw Exception("Server requests unsupported authentication method: ${handshake.auth}")
        }
      }
      else -> throw Exception("Unsupported authentication method: ${handshake.auth}")
    }
  }

  private fun nativePassword(password: String, salt: ByteArray): ByteArray {
    if (password.isEmpty()) return ByteArray(0)
    val sha1 = MessageDigest.getInstance("SHA1")
    val s = sha1.digest(password.toByteArray())
    val ss = sha1.digest(s)
    sha1.update(salt)
    val xor = sha1.digest(ss)
    return ByteArray(s.size, { i ->
      (s[i].toInt() xor xor[i].toInt()).toByte()
    })
  }

  class Exception(message: String): RuntimeException(message)

  sealed class Credentials(internal val username: String): MysqlCredentials {

    class UnsecuredCredentials(username: String = "root"): Credentials(username)
    class PasswordCredentials(username: String = "root",
                              internal val password: String = ""): Credentials(username)

    override suspend fun connectTo(database: String) = connectTo(
      database, InetSocketAddress(InetAddress.getLoopbackAddress(), 3306), 4194304
    )

    override suspend fun connectTo(database: String, bufferSize: Int) = connectTo(
      database, InetSocketAddress(InetAddress.getLoopbackAddress(), 3306), bufferSize
    )

    override suspend fun connectTo(database: String, address: InetAddress) = connectTo(
      database, InetSocketAddress(address, 3306), 4194304
    )

    override suspend fun connectTo(database: String, address: InetAddress, bufferSize: Int) = connectTo(
      database, InetSocketAddress(address, 3306), bufferSize
    )

    override suspend fun connectTo(database: String, address: SocketAddress) = connectTo(
      database, address, 4194304
    )

    override suspend fun connectTo(database: String, address: SocketAddress,
                                   bufferSize: Int): MysqlConnection {
      return MysqlConnection.to(database, this, address, bufferSize)
    }

  }

}
