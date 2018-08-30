package info.jdavid.asynk.mysql

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.security.KeyFactory
import java.security.MessageDigest
import java.security.spec.X509EncodedKeySpec
import java.util.Base64
import javax.crypto.Cipher

typealias MysqlCredentials=info.jdavid.asynk.sql.Credentials<MysqlConnection>

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
        connection.send(
          Packet.HandshakeResponse(0x01.toByte(), database, credentials.username, authResponse, handshake)
        )
        val switch = connection.receive(Packet.AuthSwitchRequest::class.java)
        assert(switch.sequenceId == 2.toByte())
        if (switch.auth != null) {
          throw Exception("Server requests unsupported authentication method: ${handshake.auth}")
        }
      }
      "sha256_password" -> {
        if (credentials !is Credentials.PasswordCredentials) throw Exception("Incompatible credentials.")
        connection.send(
          Packet.HandshakeResponse(
            0x01.toByte(), database, credentials.username, byteArrayOf(0x01.toByte()), handshake
          )
        )
        val authData = connection.receive(Packet.AuthMoreData::class.java)
        val publicKey = authData.data
        val authResponse = sha256Password(credentials.password, handshake.scramble, publicKey)
        connection.send(
          Packet.AuthResponse(authData.sequenceId.inc(), authResponse)
        )
        val switch = connection.receive(Packet.AuthSwitchRequest::class.java)
        if (switch.auth != null) {
          throw Exception("Server requests unsupported authentication method: ${handshake.auth}")
        }
      }
      "caching_sha2_password" -> {
        if (credentials !is Credentials.PasswordCredentials) throw Exception("Incompatible credentials.")
        val authResponse = cachingSha2Password(credentials.password, handshake.scramble)
        connection.send(
          Packet.HandshakeResponse(0x01.toByte(), database, credentials.username, authResponse, handshake)
        )
        val result = connection.receive(Packet.AuthReadResult::class.java)
        if (!result.complete) {
          connection.send(Packet.PublicKeyRetrieval(result.sequenceId.inc(), true))
          val authData = connection.receive(Packet.AuthMoreData::class.java)
          val publicKey = authData.data
          val authResponse2 = sha256Password(credentials.password, handshake.scramble, publicKey)
          connection.send(
            Packet.AuthResponse(authData.sequenceId.inc(), authResponse2)
          )
        }
        val switch = connection.receive(Packet.AuthSwitchRequest::class.java)
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
    return ByteArray(s.size) { i ->
      (s[i].toInt() xor xor[i].toInt()).toByte()
    }
  }

  private fun sha256Password(password: String, salt: ByteArray, publicKey: String): ByteArray {
    if (password.isEmpty()) return ByteArray(0)
    val rsa = KeyFactory.getInstance("RSA")
    val key = rsa.generatePublic(
      X509EncodedKeySpec(
        Base64.getDecoder().decode(
          publicKey.substring(
            publicKey.indexOf('\n') + 1,
            publicKey.lastIndexOf('\n', publicKey.length - 4)
          ).replace("\n","").replace("\r", "")
        )
      )
    )
    val cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding")
    cipher.init(Cipher.ENCRYPT_MODE, key)
    return cipher.doFinal(
      byteArrayOf(*password.toByteArray(), 0x00.toByte()).apply {
        for (i in 0 until size) {
          set(i, (get(i).toInt() xor salt[i % salt.size].toInt()).toByte())
        }
      }
    )
  }

  private fun cachingSha2Password(password: String, salt: ByteArray): ByteArray {
    if (password.isEmpty()) return ByteArray(0)
    val sha256 = MessageDigest.getInstance("SHA-256")
    val s = sha256.digest(password.toByteArray())
    val ss = sha256.digest(s)
    sha256.update(ss)
    val xor = sha256.digest(salt)
    return ByteArray(s.size) { i ->
      (s[i].toInt() xor xor[i].toInt()).toByte()
    }
  }

  class Exception(message: String): RuntimeException(message)

  /**
   * Implementations of the different credentials available for Mysql and MariaDB.
   */
  sealed class Credentials(internal val username: String): MysqlCredentials {

    /**
     * Username only (no password) unsecured credentials.
     * @param username the database username (root by default).
     */
    class UnsecuredCredentials(username: String = "root"): Credentials(username)

    /**
     * Username/password credentials.
     * @param username the database username (root by default).
     * @param password the user password (blank by default).
     */
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
