package simpledb.buffer

import simpledb.filemanagement.{FileManager, Page, BlockId}
import simpledb.log.LogManager

class Buffer(
    val fm: FileManager,
    val lm: LogManager
) {
  private var _blockId: BlockId = null
  private var pins = 0
  private var txnum = 0
  private var lsn = -1
  val contents: Page = Page(fm.blockSize)

  def blockId: BlockId = _blockId

  def setModified(txnum: Int, lsn: Int): Unit = {
    this.txnum = txnum
    if (lsn >= 0) {
      this.lsn = lsn
    }
  }

  def isPinned: Boolean = pins > 0

  def modifyingTx: Int = txnum

  def pin(): Unit = {
    pins += 1
  }

  def unpin(): Unit = {
    pins -= 1
  }

  def assignToBlock(b: BlockId): Unit = {
    flush()
    _blockId = b
    fm.read(b, contents)
  }

  def flush(): Unit = {
    if (lsn >= 0) {
      lm.flush(lsn)
      fm.write(_blockId, contents)
      txnum = -1
    }
  }
}
