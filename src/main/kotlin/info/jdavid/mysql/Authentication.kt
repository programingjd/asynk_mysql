package info.jdavid.mysql

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.security.MessageDigest

object Authentication {

  internal suspend fun authenticate(connection: Connection,
                                    database: String,
                                    credentials: Credentials): List<Packet.FromServer> {
    val messages = connection.receive()
    if (messages.find { it is Packet.ErrPacket } != null) throw RuntimeException("Authentication failed.")
    if (messages.find { it is Packet.OKPacket && it.sequenceId == 2.toByte() } != null) return messages
    val handshake = messages.find { it is Packet.HandshakePacket } as? Packet.HandshakePacket ?:
                                throw RuntimeException("Expected handshake package.")
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
        return authenticate(connection, database, credentials)
      }
      else -> throw Exception("Unsupported authentication method: ${handshake.auth}")
    }
  }

  private fun nativePassword(password: String, salt: ByteArray): ByteArray {
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

  sealed class Credentials(internal val username: String) {

    class UnsecuredCredentials(username: String = "root"): Credentials(username)
    class PasswordCredentials(username: String = "root",
                              internal val password: String = ""): Credentials(username)

    suspend fun connectTo(
      database: String,
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress (), 3306)
    ): Connection {
      return Connection.to(database, this, address)
    }

  }

}
