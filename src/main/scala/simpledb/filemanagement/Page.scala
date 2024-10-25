package simpledb.filemanagement

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class Page(bb: ByteBuffer) {
  import Page.Charset

  def getInt(offset: Int): Int = bb.getInt(offset)

  def setInt(offset: Int, n: Int): Unit = bb.putInt(offset, n)

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
