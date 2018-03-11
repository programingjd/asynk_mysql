package info.jdavid.mysql

internal object Types {

  val FLOAT = 0x04.toByte()
  val DOUBLE = 0x05.toByte()

  val NULL = 0x06.toByte()

  val BYTE = 0x01.toByte()      //  TINYINT
  val SHORT = 0x02.toByte()     //  SMALLINT
  val INT = 0x03.toByte()
  val INT24 = 0x09.toByte()     //  MEDIUMINT
  val LONG = 0x08.toByte()      //  BIGINT

  val DECIMAL = 0x00.toByte()
  val NUMERIC = 0xf6.toByte()   //  NEWDECIMAL

  val TIMESTAMP = 0x07.toByte()
  val TIMESTAMP2 = 0x11.toByte()
  val DATETIME = 0x0c.toByte()
  val DATETIME2 = 0x12.toByte()
  val DATE = 0x0a.toByte()
//  val TIME = 0x0b.toByte()
//  val TIME2 = 0x13.toByte()
//  val YEAR = 0x0d.toByte()
//  val NEWDATE = 0x0e.toByte()

  val VARCHAR = 0x0f.toByte()
  val VARSTRING = 0xfd.toByte()
  val STRING = 0xfe.toByte()

  val TINYBLOB = 0xf9.toByte()
  val MEDIUMBLOB = 0xfa.toByte()
  val LONGBLOB = 0xfb.toByte()
  val BLOB = 0xfc.toByte()

  val BIT = 0x10.toByte()

  val ENUM = 0xf7.toByte()
  val SET = 0xf8.toByte()

  val GEOMETRY = 0xff.toByte()

  val JSON = 0xf5.toByte()

}
