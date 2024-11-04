package simpledb.log

import simpledb.filemanagement.{Page, FileManager, BlockId}

class LogManager (
  private val fm: FileManager,
  private val logFile: String,
) {
  private val logPage: Page = {
    val b = new Array[Byte](fm.blockSize)
    Page(b)
  }
  private val logSize = fm.size(logFile)
  private var currentBlock =
    if (logSize == 0) {
      appendNewBlock()
    } else {
      val currentBlock = BlockId(logFile, logSize - 1)
      fm.read(currentBlock, logPage)
      currentBlock
    }

  private var latestLSN= 0;
  private var lastSavedLSN = 0;

  def flush(lsn: Int): Unit = {
    if (lsn >= lastSavedLSN) {
      flush()
    }
  }

  def iterator(): Iterator[Array[Byte]] = {
    flush()
    ???
  }

  def append(logRec: Array[Byte]): Int = synchronized {
    var boundary = logPage.getInt(0)
    val recSize = logRec.length
    val bytesNeeded = recSize + java.lang.Integer.BYTES
    if (boundary - bytesNeeded < java.lang.Integer.BYTES) { // It doesn't fit
      flush() // so move to the next block
      currentBlock = appendNewBlock()
    }
    val recPos = boundary - bytesNeeded
    logPage.setBytes(recPos, logRec)
    logPage.setInt(0, recPos) // the new boudary
    latestLSN += 1
    latestLSN
  }

  private def appendNewBlock(): BlockId = {
    val block = fm.append(logFile)
    logPage.setInt(0, fm.blockSize)
    fm.write(block, logPage)
    block
  }

  private def flush() = {
    fm.write(currentBlock, logPage);
    lastSavedLSN = latestLSN
  }
}
