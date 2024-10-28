package simpledb.filemanagement

import scala.io.Source
import java.io.File
import java.nio.file.{Files, FileVisitor, Path, FileVisitResult, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes

import scala.jdk.CollectionConverters.*

import simpledb.filemanagement.FileManager

class FileManagerTest extends munit.FunSuite {
  val DbDirectory = FunFixture[File](
    setup = { test =>
      Files.createTempDirectory(s"dbdir-${test.name}").toFile()
    },
    teardown = { file =>
      val files = Files.walkFileTree(file.toPath, new SimpleFileVisitor[Path] {
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
      })
    }
  )

  DbDirectory.test("FileManager#write, read") { dbDir =>
    val fileManager = FileManager(dbDir, 1024)
    val blockId = BlockId("test-a", 0)
    val page = Page(256)
    page.setInt(0, 42)
//    page.setString(4, "hello")
    fileManager.write(blockId, page)

    val readPage = Page(256)
    fileManager.read(blockId, readPage)
    assertEquals(readPage.getInt(0), 42)
//    assertEquals(readPage.getString(4), "hello")
//    assertEquals(readPage.getBytes(10).toList, List[Byte](1, 2, 3, 4, 5))
  }
}
