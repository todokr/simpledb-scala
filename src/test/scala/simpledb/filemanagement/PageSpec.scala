package simpledb.filemanagement

class PageTest extends munit.FunSuite {

  test("Page#setInt, getInt") {
    val page = Page(8)
    page.setInt(0, Int.MaxValue)
    page.setInt(4, Int.MinValue)
    assertEquals(page.getInt(0), Int.MaxValue)
    assertEquals(page.getInt(4), Int.MinValue)
  }

  test("Page#setLong, getLong") {
    val page = Page(16)
    page.setLong(0, Long.MaxValue)
    page.setLong(8, Long.MinValue)
    assertEquals(page.getLong(0), Long.MaxValue)
    assertEquals(page.getLong(8), Long.MinValue)
  }

  test("Page#setString, getString") {
    val page = Page(20)
    page.setString(0, "hello, ")
    page.setString(11, "world")
    assertEquals(page.getString(0), "hello, ")
    assertEquals(page.getString(11), "world")
  }

  test("Page#setBytes, getBytes") {
    val page = Page(16)
    page.setBytes(0, Array(Byte.MinValue, Byte.MaxValue))
    page.setBytes(6, Array(Byte.MaxValue, Byte.MinValue))
    assertEquals(page.getBytes(0).toSeq, Array(Byte.MinValue, Byte.MaxValue).toSeq)
    assertEquals(page.getBytes(6).toSeq, Array(Byte.MaxValue, Byte.MinValue).toSeq)
  }
}
