from modules import *


def report(name_file, minn_price=1):
    """
    Возвращает отчет записанный в файл report.txt
    minn_price - значение для вывода всех покупок, где цена товара больше или равна min_price;
    name_file - имя исходного файла
    """
    purchases_file = file_processing(name_file)

    list_tasks = [
        total_revenue(purchases_file),
        items_by_category(purchases_file),
        expensive_purchases(purchases_file, minn_price),
        average_price_by_category(purchases_file),
        most_frequent_category(purchases_file)
    ]

    with open("report.txt", "a") as file:
        for task in list_tasks:
            print(task) # вывод в консоль
            file.write(task + '\n')
        print("Отчет записан в файл 'report.txt'")


if __name__ == "__main__":
    report("purchases.txt")
