// Задание 2. Напишите функцию, которая принимает
// два целых числа и возвращает их сумму.
object Task2 {
  def sum(a: Int, b: Int): Int = a + b

  def main(args: Array[String]): Unit = {
    val result = sum(5, 3)

    println(s"Сумма 5 и 3 = $result")
  }
}
