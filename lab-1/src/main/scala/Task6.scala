// Задание 6. Напишите функцию, которая принимает
// список строк и возвращает новую строку, состоящую
// из всех строк списка, разделенных пробелами.
object Task6 {
  private def joinStrings(strings: List[String]): String = strings.mkString(" ")

  def main(args: Array[String]): Unit = {
    val words = List("Привет", "мир", "из", "Scala")
    val result = joinStrings(words)

    println(result)
  }
}
