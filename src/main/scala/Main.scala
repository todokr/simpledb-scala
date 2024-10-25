@main def hello(): Unit = {
  println(x = """|Hello world!""".stripMargin)
  println(msg)
}

def msg: String = "I was compiled by Scala 3. :)"
