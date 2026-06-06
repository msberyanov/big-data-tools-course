# Отчёт по лабораторной работе 1.

---

## Задание 1.

**Цель**: Написать программу, которая принимает имя пользователя с клавиатуры и выводит приветственное сообщение.

**Реализация**:
```scala
object Task1 extends App {
  print("Введите ваше имя: ")
  val name = scala.io.StdIn.readLine()
  println(s"Привет, $name!")
}
```

**Результат выполнения** (при вводе "Максим Берьянов"):
```
Привет, Максим Берьянов!
```

---

## Задание 2.

**Цель**: Написать функцию, которая принимает два целых числа и возвращает их сумму.

**Реализация**:
```scala
object Task2 {
  def sum(a: Int, b: Int): Int = a + b

  def main(args: Array[String]): Unit = {
    val result = sum(5, 3)
    println(s"Сумма 5 и 3 = $result")
  }
}
```

**Результат выполнения**:
```
Сумма 5 и 3 = 8
```

---

## Задание 3.

**Цель**: Создать список чисел и применить функцию, увеличивающую каждое число на 1.

**Реализация**:
```scala
object Task3 {
  def main(args: Array[String]): Unit = {
    val numbers = List(1, 2, 3, 4, 5)
    val incremented = numbers.map(_ + 1)
    println(incremented)
  }
}
```

**Результат выполнения**:
```
List(2, 3, 4, 5, 6)
```

---

## Задание 4.

**Цель**: Программа принимает число с клавиатуры и выводит, является ли оно чётным или нечётным.

**Реализация**:
```scala
object Task4 {
  def main(args: Array[String]): Unit = {
    print("Введите число: ")
    val number = scala.io.StdIn.readInt()
    
    if (number % 2 == 0) {
      println(s"$number - чётное число")
    } else {
      println(s"$number - нечётное число")
    }
  }
}
```

**Пример результата** (при вводе "7"):
```
7 - нечётное число
```

---

## Задание 5.

**Цель**: Программа принимает строку и выводит её длину.

**Реализация**:
```scala
object Task5 {
  def main(args: Array[String]): Unit = {
    print("Введите строку: ")
    val input = scala.io.StdIn.readLine()
    println(s"Длина строки: ${input.length}")
  }
}
```

**Пример результата** (при вводе "Привет мир из Scala"):
```
Длина строки: 19
```

---

## Задание 6.

**Цель**: Написать функцию, которая принимает список строк и возвращает новую строку, состоящую из всех строк, разделённых пробелами.

**Реализация**:
```scala
object Task6 {
  private def joinStrings(strings: List[String]): String = strings.mkString(" ")
  
  def main(args: Array[String]): Unit = {
    val words = List("Привет", "мир", "из", "Scala")
    val result = joinStrings(words)
    println(result)
  }
}
```

**Результат выполнения**:
```
Привет мир из Scala
```

## Примечание.

Для выполнения задач используется утилита sbt:

```bash
sbt
```

Затем внутри sbt консоли необходимо выбрать одну из команд для запуска конкретной задачи:

```bash
runMain Task1
runMain Task2
runMain Task3
runMain Task4
runMain Task5
runMain Task6
```

Для выхода из консоли sbt используется команда `exit`.