# Итоговое задание  №7  
Используя полученные в этом разделе знания, решите следующую задачу. Немного изменим правила.

Теперь код необходимо не написать, а исправить. 

Вам дан фрагмент кода, который выполняет операции над списком строк. Код написан в императивном стиле, использует изменяемые переменные и циклы. Ваша задача — преобразовать этот код в функциональный стиль. 

**Как? Решайте сами.**

```object StringProcessor {
  def processStrings(strings: List[String]): List[String] = {
    var result = List[String]()
    for (str <- strings) {
      if (str.length > 3) {
        result = result :+ str.toUpperCase
      }
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val strings = List("apple", "cat", "banana", "dog", "elephant")
    val processedStrings = processStrings(strings)
    println(s"Processed strings: $processedStrings")
  }
}```