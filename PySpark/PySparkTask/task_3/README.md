# Общее задание №1.   
Генератор синтетических данных для наполнения контура DEV  
Для начала дадим определение тому, что такое синтетические данные.

Синтетические данные — это искусственно созданные данные, которые имитируют статистические характеристики достоверных данных.

Теперь предистория. Во многих банках очень часто разработка ведется для нескольких контуров. То есть грубо говоря DEV и PROD, но может быть и больше. Поскольку мы с Вами ребята умные, то понимаем, что внедрить что-либо в PROD не представляется возможным и не является чем-то хорошим. А теперь собственно реалии.

![](img.png)


Приходит к нам DE, но покруче и поважнее. И рассказывает про свою интересную задачу. Вкратце - у него есть скрипт, который выполняет аналитические операции с большими данными в PROD среде. Но, так получилось, что эти данные есть только в PROD среде. То есть ни скопировать их, ни переместить в DEV среду не представляется возможным. И прилетает нам первая (ураааа) задача - "Подготовить DEV контур для запуска скрипта на тестовых данных". Понятно, что если бы так ставились задачи, то мы бы с Вами и степика бы не увидели, поэтому давайте расшифруем, что собственно хочет от нас middle (а может и senior) DE разработчик.

Есть следующая таблица в PROD контуре, которая уже воссоздана в DEV контуре, только данных нет. Нам особо разницы нет воссозданы ли данные структуры в БД или где-то еще. Главное - нагенерировать синтетические данные в csv.

Структура следующая - 

- **id**: Уникальный идентификатор.  
- **name**: Случайное имя. Минимальное количество букв в имени - 5.  
- **email**: Email, сгенерированный на основе имени. Обязательно должна быть @ и ru/com.  
- **city**: Случайный город. Минимальное количество букв в городе - 7.  
- **age**: Случайный возраст. Минимальный возраст - 18. Максимальный возраст - 95.  
- **salary**: Случайная зарплата.  
- **registration_date**: Дата регистрации, зависящая от возраста. Тут дата регистрации очевидна не должна быть меньше, чем значение в поле age.  


Тип данных для полей, разумеется должен соответствовать реалиям, как и формат данных. В качестве решения программа должна выдавать строго один файл формата .csv. То есть не должно быть никаких партиций и success файлов. Их надо удалить.

![](img_1.png)

При запуске программы обязательно необходимо ввести число генерируемых данных (количество строк). Хоть через PyQT, хоть через терминал, да можно хоть на сайте. 

**Критерии правильности (которые могут обсуждаться сообществом) :**

1. Обязательно, используйте PySpark.  
2. Использование библиотеки Faker и тому подобных нежелательно.  
3. Оптимизируйте выполнение приложения, если используете какие-то готовые словари.  
4. Название получившегося файла должно быть в формате "текущий год-месяц-день-dev.csv". То есть каждый раз при каждодневном запуске файл будет с новым названием текущего запуска.  
5. Обратите внимание, что допускаются и очень желательны значения NULL. Но не более 5 % от общего количества значений именно в этом столбце! То есть если строк в столбце 10, то количество NULL быть не должно!!! Желательно отразить данную проверку в коде, но закомментировать ее.  
6. Никаких красных строк при запуске spark приложения в начале быть не должно :)  


Отправляем файл коллеге, он загружает его в DEV контур и выполняет там свой скрипт. Было бы неплохо поставить это дело на каждодневное выполнение (да и ручками файлик загружать тоже не очень хочется, а хочется все напрямую в БД передавать) . Но об этом мы узнаем в главе, посвященной оркестраторам.