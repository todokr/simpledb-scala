package simpledb.log

import java.nio.file.Files
import simpledb.filemanagement.{Page, FileManager, BlockId}


class LogManagerTest extends munit.FunSuite {
  val dbDir = Files.createTempDirectory("dbdir").toFile()
  val fm = FileManager(dbDir, 24)

  test("LogManager#append") {
    val logManager = new LogManager(fm, "testfile")
    val log1 = Array[Byte](1, 2, 3, 4, 5)
    //val lsn1 = logManager.append(log1)
//    assertEquals(lsn1, 1)
  }

  test("LogManager#iterator") {
    val logManager = new LogManager(fm, "testfile")
    val log1 = Array[Byte](1, 2, 3, 4, 5)
    val log2 = Array[Byte](6, 7, 8, 9, 10)
    logManager.append(log1)
    logManager.append(log2)
    val iter = logManager.iterator()
    // FILO order
    assertEquals(iter.hasNext, true)
    assertEquals(iter.next().toSeq, log2.toSeq)
    assertEquals(iter.hasNext, true)
    assertEquals(iter.next().toSeq, log1.toSeq)
    assertEquals(iter.hasNext, false)
  }
}

