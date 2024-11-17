package simpledb.buffer

import java.io.File
import java.nio.file.{Files, Path, FileVisitResult, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes

import simpledb.filemanagement.{FileManager, BlockId}
import simpledb.log.LogManager

class BufferManagerSpec extends munit.FunSuite {
  val DbDirectory = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(s"dbdir-${test.name}").toFile()
    },
    teardown = { file =>
      val files = Files.walkFileTree(
        file.toPath,
        new SimpleFileVisitor[Path] {
          override def visitFile(
              file: Path,
              attrs: BasicFileAttributes
          ): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(
              dir: Path,
              exc: java.io.IOException
          ): FileVisitResult = {
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
    }
  )

  DbDirectory.test("BufferManager#pin") { dbDir =>
    val fm = FileManager(dbDir, 1024)
    val lm = new LogManager(fm, "testlog")
    val bm = new BufferManager(fm, lm, 3)

    val blk1 = BlockId("testfile", 0)
    val buff1 = bm.pin(blk1)
    assertEquals(bm.available, 2)

    val blk2 = BlockId("testfile", 1)
    val buff2 = bm.pin(blk2)
    assertEquals(bm.available, 1)

    val blk3 = BlockId("testfile", 2)
    val buff3 = bm.pin(blk3)
    assertEquals(bm.available, 0)
  }

  DbDirectory.test("BufferManager#unpin") { dbDir =>
    val fm = FileManager(dbDir, 1024)
    val lm = new LogManager(fm, "testlog")
    val bm = new BufferManager(fm, lm, 3)

    val blk1 = BlockId("testfile", 0)
    val buff1 = bm.pin(blk1)
    assertEquals(bm.available, 2)

    bm.unpin(buff1)
    assertEquals(bm.available, 3)
  }

  DbDirectory.test("modification is written to disk") { dbDir =>
    val fm = FileManager(dbDir, 1024)
    val lm = new LogManager(fm, "testlog")
    val bm = new BufferManager(fm, lm, 3)

    val blk1 = BlockId("testfile", 1)
    val buff1 = bm.pin(blk1)
    val p1 = buff1.contents
    p1.setInt(0, 42)
    buff1.setModified(1, 0)
    bm.unpin(buff1)

    // One of these pins will flush buff1 to disk:
    val buff2 = bm.pin(BlockId("testfile", 2))
    val buff3 = bm.pin(BlockId("testfile", 3))
    val buff4 = bm.pin(BlockId("testfile", 4))

    bm.unpin(buff2)
    val newBuff2 = bm.pin(blk1)
    val shouleHaveBeenWritten = newBuff2.contents

    assertEquals(shouleHaveBeenWritten.getInt(0), 42)
  }
}
