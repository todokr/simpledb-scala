package simpledb.filemanagement

class PageTest extends munit.FunSuite {
  test("Page#setInt, getInt") {
    val page = Page(256)
    page.setInt(0, 42)
    assertEquals(page.getInt(0), 42)
  }

  test("Page#setString, getString") {
    val page = Page(256)
    page.setString(4, "hello")
    assertEquals(page.getString(4), "hello")
  }

  test("Page#setBytes, getBytes") {
    val page = Page(256)
    page.setBytes(10, Array[Byte](1, 2, 3, 4, 5))
    assertEquals(page.getBytes(10).toList, List[Byte](1, 2, 3, 4, 5))
  }
}
