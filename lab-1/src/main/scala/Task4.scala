// Задание 4. Напишите программу, которая принимает
// число с клавиатуры и выводит, является ли оно
// четным или нечетным.
object Task4 {
  def main(args: Array[String]): Unit = {
    print("Введите число: ")

    val number = scala.io.StdIn.readInt()

    if (number % 2 == 0) {
      println(s"$number - четное число")
    } else {
      println(s"$number - нечетное число")
    }
  }
}
