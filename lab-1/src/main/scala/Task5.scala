// Задание 5. Создайте программу,
// которая принимает строку и выводит её длину.
object Task5 {
  def main(args: Array[String]): Unit = {
    print("Введите строку: ")

    val input = scala.io.StdIn.readLine()

    println(s"Длина строки: ${input.length}")
  }
}
