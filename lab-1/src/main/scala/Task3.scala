// Задание 3. Создайте список из нескольких чисел
// (например, List(1, 2, 3, 4, 5)) и примените
// к нему функцию, которая увеличивает каждое
// число на 1. Выведите получившийся список на экран.
object Task3 {
  def main(args: Array[String]): Unit = {
    val numbers = List(1, 2, 3, 4, 5)
    val incremented = numbers.map(_ + 1)

    println(incremented)
  }
}
