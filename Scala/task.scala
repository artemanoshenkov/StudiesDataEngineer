object StringProcessor {
  def processStrings(strings: List[String]): List[String] = {
// Изменил преобразование строк в списке, создав новый неизменяемый список с помощью функций:
// filter(фильтрует по длине строки)
// map(преобразует каждый элемент в новом списке)
//
    val result = strings.filter(str => str.length > 3).map(str => str.toUpperCase)
    result
  }

  def main(args: Array[String]): Unit = {
    val strings = List("apple", "cat", "banana", "dog", "elephant")
    val processedStrings = processStrings(strings)
    println(s"Processed strings: $processedStrings")
  }
}