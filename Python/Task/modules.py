import re
import json


def file_processing(name_file: str) -> list:
    """
    Подготовка файла для работы с ним
    возвращает list[dict]
    """
    g_list = []
    with open(name_file, 'r') as file:
        pattern = r'\{.*\}'
        res = re.findall(pattern, file.read())
        for i in res:
            g_list.append(json.loads(i))

    return g_list


def total_revenue(purchases: list[dict]) -> str:
    # Рассчитайте и верните общую выручку (цена * количество для всех записей).
    result = 0
    for i in purchases:
        result += i['price'] * i['quantity']
    return "Общая выручка: {}".format(result)


def items_by_category(purchases: list[dict]) -> str:
    # Верните словарь, где ключ — категория, а значение — список уникальных товаров в этой категории.
    result_dict = {}
    for i in purchases:
        category = i["category"]
        if category not in result_dict:
            result_dict[category] = [i["item"]]
        else:
            if i['item'] not in result_dict[category]:
                result_dict[category].append(i["item"])

    return "Товары по категориям: {}".format(result_dict)


def expensive_purchases(purchases: list[dict], min_price: int | float) -> str:
    # Выведите все покупки, где цена товара больше или равна min_price.
    result_list = []
    for i in purchases:
        if i["price"] >= min_price:
            result_list.append(i)

    return "Покупки дороже {price}: {product}".format(price=float(min_price), product=result_list)


def average_price_by_category(purchases) -> str:
    # Рассчитайте среднюю цену товаров по каждой категории.
    price_dict = {}
    result_dict = {}
    for i in purchases:

        category = i["category"]
        if category not in price_dict:
            price_dict[category] = [i["price"]]
        else:
            price_dict[category].append(i["price"])

    for category in price_dict:
        result_dict[category] = sum(price_dict[category]) / len(price_dict[category])

    return "Средняя цена по категориям: {}".format(result_dict)

def most_frequent_category(purchases) -> str:
#Найдите и верните категорию, в которой куплено больше всего единиц товаров (учитывайте поле quantity).
    result_dict = {}
    for i in purchases:
        category = i["category"]
        if category not in result_dict:
            result_dict[category] = i["quantity"]
        else:
            result_dict[category] += i["quantity"]

    return "Категория с наибольшим количеством проданных товаров: {}".format(max(result_dict, key=result_dict.get))