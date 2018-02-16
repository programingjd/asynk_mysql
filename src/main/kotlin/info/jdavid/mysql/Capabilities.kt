package info.jdavid.mysql

internal object Capabilities {

  val CLIENT_MYSQL = 1 shl 0

  val CONNECT_WITH_DB = 1 shl 3

  val IGNORE_SPACE = 1 shl 8

  val CLIENT_PROTOCOL_41 = 1 shl 9

  val CLIENT_INTERACTIVE = 1 shl 10

  val TRANSACTIONS = 1 shl 12

   val SECURE_CONNECTION = 1 shl 13

  val PLUGIN_AUTH = 1 shl 19

  val CLIENT_DEPRECATE_EOF = 1 shl 24

  fun clientCapabilities() = CLIENT_MYSQL +
                             CONNECT_WITH_DB +
                             IGNORE_SPACE +
                             CLIENT_PROTOCOL_41 +
                             TRANSACTIONS +
                             SECURE_CONNECTION +
                             PLUGIN_AUTH +
                             CLIENT_DEPRECATE_EOF

}
