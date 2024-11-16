package simpledb.filemanagement

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class Page private (bb: ByteBuffer) {
  import Page.Charset

  def getInt(offset: Int): Int = bb.getInt(offset)

  def setInt(offset: Int, n: Int): Unit = bb.putInt(offset, n)

  def getLong(offset: Int): Long = bb.getLong(offset)

  def setLong(offset: Int, n: Long): Unit = bb.putLong(offset, n)

  def getBytes(offset: Int): Array[Byte] = {
    bb.position(offset)
    val length = bb.getInt()
    val bytes = new Array[Byte](length)
    bb.get(bytes)
    bytes
  }

  def setBytes(offset: Int, bytes: Array[Byte]): Unit = {
    bb.position(offset)
    bb.putInt(bytes.length)
    bb.put(bytes)
  }

  def getString(offset: Int): String = {
    val bytes = getBytes(offset)
    new String(bytes, Charset)
  }

  def setString(offset: Int, s: String): Unit = {
    val bytes = s.getBytes(Charset)
    setBytes(offset, bytes)
  }

  private[filemanagement] def contents: ByteBuffer = {
    bb.position(0)
    bb
  }

  override def toString(): String = {
    val sb = new StringBuilder
    for (i <- 0 until bb.limit()) {
      sb.append(f"${bb.get(i)}%02x")
      if (i < bb.limit() - 1) sb.append(",")
    }
    sb.toString()
  }
}

object Page {
  private val Charset = StandardCharsets.US_ASCII

  /** A constractor for creating data buffers */
  def apply(blockSize: Int): Page = {
    val bb = ByteBuffer.allocateDirect(blockSize)
    new Page(bb)
  }

  /** A constructor for creating og pages */
  def apply(b: Array[Byte]): Page = {
    val bb = ByteBuffer.wrap(b)
    new Page(bb)
  }

  def maxLength(strlen: Int): Int = {
    val bytesPerChar = Charset.newEncoder().maxBytesPerChar().toInt
    Integer.BYTES + strlen * bytesPerChar
  }
}
