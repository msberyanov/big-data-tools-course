// Задание #1. Напишите программу на Scala,
// которая принимает имя пользователя с клавиатуры
// и выводит приветственное сообщение.
object Task1 extends App {
  print("Введите ваше имя: ")

  val name = scala.io.StdIn.readLine()

  println(s"Привет, $name!")
}