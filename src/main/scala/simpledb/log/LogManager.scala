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
    new LogIterator(fm, currentBlock)
  }

  def append(logRec: Array[Byte]): Int = synchronized {
    var boundary = logPage.getInt(0)
    val recSize = logRec.length
    val bytesNeeded = recSize + java.lang.Integer.BYTES
    if (boundary - bytesNeeded < java.lang.Integer.BYTES) { // It doesn't fit
      flush() // so move to the next block
      currentBlock = appendNewBlock()
      boundary = logPage.getInt(0)
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

/** A class that provides the ability to move through the records of the log file in reverse order. */
class LogIterator (
  private val fm: FileManager,
  private val blockId: BlockId
) extends Iterator[Array[Byte]] {
  private val page = Page(new Array[Byte](fm.blockSize))
  private var boundary = page.getInt(0)
  private var currentPos = boundary

  moveToBlock(blockId)

  override def hasNext: Boolean =
    currentPos < fm.blockSize || // current block内にまだ読むべきものがある
    blockId.blockNum > 0; // まだblockNumが若いblockがある

  override def next(): Array[Byte] = {
    if (currentPos == fm.blockSize) { // ブロックの最後に到達した
      val blk = BlockId(blockId.fileName, blockId.blockNum - 1)
      moveToBlock(blk)
    }
    val rec = page.getBytes(currentPos)
    currentPos += java.lang.Integer.BYTES + rec.length
    rec
  }

  private def moveToBlock(blockId: BlockId) = {
    fm.read(blockId, page)
    boundary = page.getInt(0)
    currentPos = boundary
  }
}
