package simpledb.filemanagement

import java.io.File
import java.io.RandomAccessFile

import scala.collection.mutable

class FileManager private (
  private val dbDirectory: File,
  private val blockSize: Int,
  private val isNew: Boolean,
  private val openFiles: mutable.Map[String, RandomAccessFile] = mutable.Map()
) {

  def read(block: BlockId, page: Page): Unit = this.synchronized {
    try {
      val f = getFile(block.fileName)
      f.seek(block.blockNum * blockSize)
      f.getChannel.read(page.contents)
    } catch {
      case e: Exception => throw new RuntimeException(s"cannot read block $block", e)
    }
  }

  def write(block: BlockId, page: Page): Unit = this.synchronized {
    try {
      val f = getFile(block.fileName)
      f.seek(block.blockNum * blockSize)
      f.getChannel.write(page.contents)
    } catch {
      case e: Exception => throw new RuntimeException(s"cannot write block $block", e)
    }
  }

  /** ブロックを拡張する */
  def append(fileName: String): BlockId = this.synchronized {
    val newBlockNum = size(fileName)
    val newBlockId = BlockId(fileName, newBlockNum)
    try {
      val f = getFile(fileName)
      f.seek(newBlockId.blockNum * blockSize)
      val b = new Array[Byte](blockSize)
      f.write(b)
    } catch {
      case e: Exception => throw new RuntimeException(s"cannot append block $newBlockId", e)
    }
    newBlockId
  }

  private def getFile(fileName: String): RandomAccessFile = {
    openFiles.get(fileName) match {
      case Some(file) => file
      case None =>
        val file = new RandomAccessFile(new File(dbDirectory, fileName), "rws")
        openFiles.put(fileName, file)
        file
    }
  }

  private def size(fileName: String): Int = try {
    val f = getFile(fileName)
    (f.length() / blockSize).toInt
  } catch {
    case e: Exception => throw new RuntimeException(s"cannot access $fileName", e)
  }
}

object FileManager {
  def apply(dbDirectory: File, blockSize: Int): FileManager = {
    val isNew = !dbDirectory.exists()
    if (isNew) dbDirectory.mkdir()
    for (f <- dbDirectory.list() if f.startsWith("temp")) {
      val file = new File(dbDirectory, f)
      file.delete()
    }

    new FileManager(dbDirectory, blockSize, isNew)
  }
}


