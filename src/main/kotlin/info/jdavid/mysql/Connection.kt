package info.jdavid.mysql

import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.toList
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.*

class Connection internal constructor(private val channel: AsynchronousSocketChannel,
                                      private val buffer: ByteBuffer): Closeable {

  override fun close() = channel.close()

//  internal suspend fun send(message: Message.FromClient) {
//    message.writeTo(buffer.clear())
//    channel.aWrite(buffer.flip(), 5000L, TimeUnit.MILLISECONDS)
//  }
//
//  internal suspend fun receive(): List<Message.FromServer> {
//    buffer.clear()
//    val n = channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
//    if (n == buffer.capacity()) throw RuntimeException("Connection buffer too small.")
//    buffer.flip()
//    val list = LinkedList<Message.FromServer>()
//    while(buffer.remaining() > 0) {
//      val message = Message.fromBytes(buffer)
//      list.add(message)
//    }
//    return list
//  }

  companion object {
    suspend fun to(
      database: String,
      credentials: Authentication.Credentials = Authentication.Credentials.UnsecuredCredentials(),
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 5432)
    ): Connection {
      val channel = AsynchronousSocketChannel.open()
      try {
        channel.aConnect(address)
        val buffer = ByteBuffer.allocateDirect(4194304) // needs to hold any RowData message
        val connection = Connection(channel, buffer)
        val n = channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
        buffer.flip()
        val protocolVersion = buffer.get()
        val serverVersion = StringBuilder().let {
          while (true) {
            val b = buffer.get()
            if (b == 0.toByte()) break
            it.append(b.toChar())
          }
          it.toString()
        }
        val connectionId = buffer.getInt()
        val scramble1 = ByteArray(8)
        buffer.get(scramble1)
        val filler1 = buffer.get()
        val capabilities1 = BitSet.valueOf(ByteArray(2).apply { buffer.get(this) })
        val collation = buffer.get()
        val status = BitSet.valueOf(ByteArray(2).apply { buffer.get(this) })

        if (capabilities1.get(8)) {
          val capabilities2 = BitSet.valueOf(ByteArray(2).apply { buffer.get(this) })
          val len = buffer.get()
          val filler2 = ByteArray(10).apply { buffer.get(this) }
          val scramble2 = ByteArray(12).apply { buffer.get(this) }
          val filler3 = buffer.get()
        }
        else {
          val filler2 = ByteArray(13).apply { buffer.get(this) }
        }
        if (buffer.remaining() > 0) {
          val pluginData = ByteArray(buffer.remaining()).let {
            buffer.get(it)
            if (it.last() == 0.toByte()) String(it, 0, it.size - 1) else String(it)
          }
        }
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
