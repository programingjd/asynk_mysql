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
import java.util.BitSet
import java.util.LinkedList
import java.util.concurrent.TimeUnit

class Connection internal constructor(private val channel: AsynchronousSocketChannel,
                                      private val buffer: ByteBuffer): Closeable {

  val capabilities = BitSet()

  override fun close() = channel.close()

  internal suspend fun send(message: Packet.FromClient) {
    message.writeTo(buffer.clear())
    channel.aWrite(buffer.flip(), 5000L, TimeUnit.MILLISECONDS)
  }

  internal suspend fun receive(): List<Packet.FromServer> {
    buffer.clear()
    val n = channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
    if (n == buffer.capacity()) throw RuntimeException("Connection buffer too small.")
    buffer.flip()
    val list = LinkedList<Packet.FromServer>()
    while(buffer.remaining() > 0) {
      val message = Packet.fromBytes(buffer)
      list.add(message)
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
        val packet = connection.receive()
        assert(packet.size == 1)
        val handshake = packet.first()

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
  }

}
