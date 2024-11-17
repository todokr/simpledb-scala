package simpledb.buffer

import scala.util.chaining.*

import simpledb.filemanagement.{FileManager, BlockId}
import simpledb.log.LogManager

class BufferManager(
    fm: FileManager,
    lm: LogManager,
    numbuffs: Int
) {
  import BufferManager.*
  private val bufferpool = Array.fill(numbuffs)(new Buffer(fm, lm))
  private var numAvailable = numbuffs

  def available: Int = synchronized {
    numAvailable
  }

  def flushAll(txnum: Int): Unit = synchronized {
    bufferpool.filter(_.modifyingTx == txnum).foreach(_.flush())
  }

  def unpin(buff: Buffer): Unit = synchronized {
    buff.unpin()
    if (!buff.isPinned) {
      numAvailable = numAvailable + 1
      notifyAll()
    }
  }

  def pin(blk: BlockId): Buffer = synchronized {
    try {
      val timestamp = System.currentTimeMillis()
      val buff = tryToPin(blk)
      while (buff.isEmpty && !waitingTooLong(timestamp)) {
        wait(MaxTime)
        val buff = tryToPin(blk)
      }
      buff.getOrElse(throw new BufferAbortException)
    } catch {
      case _: InterruptedException => throw new BufferAbortException
    }
  }

  private def waitingTooLong(starttime: Long): Boolean = {
    System.currentTimeMillis() - starttime > MaxTime
  }

  private def tryToPin(blk: BlockId): Option[Buffer] = {
    val buffer = findExistingBuffer(blk).orElse {
      chooseUnpinnedBuffer().tap(_.foreach(_.assignToBlock(blk)))
    }

    buffer match {
      case Some(buff) if !buff.isPinned =>
        buff.pin()
        numAvailable = numAvailable - 1
        buffer
      case _ => None
    }
  }

  private def findExistingBuffer(blk: BlockId): Option[Buffer] = {
    bufferpool.find(buff => buff.blockId == blk)
  }

  private def chooseUnpinnedBuffer(): Option[Buffer] = {
    // Naive implementation
    bufferpool.find(buff => !buff.isPinned)
  }
}

object BufferManager {
  class BufferAbortException extends Exception
  private val MaxTime = 10000
}
