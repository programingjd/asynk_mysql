package info.jdavid.mysql

import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.AsynchronousSocketChannel
import java.util.LinkedList
import java.util.concurrent.TimeUnit

class Connection internal constructor(private val channel: AsynchronousSocketChannel,
                                      private val buffer: ByteBuffer): Closeable {

  override fun close() = channel.close()

  internal suspend fun send(packet: Packet.FromClient) {
    packet.writeTo(buffer.clear() as ByteBuffer)
    //warn("send: " + hex((buffer.duplicate().flip() as ByteBuffer).let { ByteArray(it.remaining()).apply { it.get(this) } }))
    channel.aWrite(buffer.flip() as ByteBuffer, 5000L, TimeUnit.MILLISECONDS)
  }

  internal suspend fun receive(): List<Packet.FromServer> {
    buffer.clear()
    val n = channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
    if (n == buffer.capacity()) throw RuntimeException("Connection buffer too small.")
    //warn("receive: " + hex((buffer.duplicate().flip() as ByteBuffer).let { ByteArray(it.remaining()).apply { it.get(this) } }))
    buffer.flip()
    val list = LinkedList<Packet.FromServer>()
    while(buffer.remaining() > 0) {
      val packet = Packet.fromBytes(buffer)
      list.add(packet)
    }
    list.forEach {
      when (it) {
        is Packet.ErrPacket -> err(it.toString())
      }
    }
    return list
  }

  companion object {
    suspend fun to(
      database: String,
      credentials: Authentication.Credentials = Authentication.Credentials.UnsecuredCredentials(),
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 5432)
    ): Connection {
      val channel = AsynchronousSocketChannel.open()
      try {
        channel.aConnect(address)
        val buffer = ByteBuffer.allocateDirect(4194304)// needs to hold any RowData message
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        val connection = Connection(channel, buffer)
        Authentication.authenticate(connection, database, credentials)
        return connection
      }
      catch (e: Exception) {
        channel.close()
        throw e
      }
    }
    private val logger = LoggerFactory.getLogger(Connection::class.java)
    private fun warn(message: String) = logger.warn(message)
    private fun err(message: String) = logger.error(message)

    internal fun hex(bytes: ByteArray): String {
      val chars = CharArray(bytes.size * 2)
      var i = 0
      for (b in bytes) {
        chars[i++] = Character.forDigit(b.toInt().shr(4).and(0xf), 16)
        chars[i++] = Character.forDigit(b.toInt().and(0xf), 16)
      }
      return String(chars)
    }
  }

}
