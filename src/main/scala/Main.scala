@main def hello(): Unit = {
  println("""|Hello world!""".stripMargin)
  println(msg)

}

/** message */
def msg: String = "I was compiled by Scala 3. :)"
