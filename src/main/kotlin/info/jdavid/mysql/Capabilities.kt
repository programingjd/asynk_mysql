package info.jdavid.mysql

internal object Capabilities {

  val CLIENT_MYSQL = 1 shl 0
  val FOUND_ROWS = 1 shl 1
  val LONG_FLAG = 1 shl 2
  val CONNECT_WITH_DB = 1 shl 3
  val NO_SCHEMA = 1 shl 4
  val COMPRESS = 1 shl 5
  val ODBC = 1 shl 6
  val LOCAL_FILES = 1 shl 7
  val IGNORE_SPACE = 1 shl 8
  val CLIENT_PROTOCOL_41 = 1 shl 9
  val CLIENT_INTERACTIVE = 1 shl 10
  val SSL = 1 shl 11
  val IGNORE_SIGPIPE = 1 shl 12
  val TRANSACTIONS = 1 shl 13
  val RESERVED = 1 shl 14
  val SECURE_CONNECTION = 1 shl 15
  val MULTI_STATEMENTS = 1 shl 16
  val MULTI_RESULTS = 1 shl 17
  val PREPARED_STATEMENT_MULTI_RESULTS = 1 shl 18
  val PLUGIN_AUTH = 1 shl 19
  val CONNECT_ATTR = 1 shl 20
  val PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 shl 21
  val PLUGIN_CAN_HANDLE_EXPIRED_PASSWORD = 1 shl 22
  val CLIENT_SESSION_TRACK = 1 shl 23
  val CLIENT_DEPRECATE_EOF = 1 shl 24

  val PROGRESS_OLD = 1 shl 29

  val MARIADB_CLIENT_PROGRESS = 1 shl 32
  val MARIADB_CLIENT_COM_MULTI = 1 shl 33


  fun clientCapabilities() = CLIENT_MYSQL +
                             CONNECT_WITH_DB +
                             //IGNORE_SPACE +
                             CLIENT_PROTOCOL_41 +
                             //TRANSACTIONS +
                             SECURE_CONNECTION +
                             PLUGIN_AUTH +
                             CLIENT_DEPRECATE_EOF

}
