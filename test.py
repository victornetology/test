# # Задание 1:
# # Напишите на C# или Python библиотеку для поставки внешним клиентам, которая умеет вычислять площадь круга по
# # радиусу и треугольника по трем сторонам.
#
# # Импортируем модуль math (предоставляет набор функций для выполнения математических операций.)
import math


def circle_area(radius):
    """
    Функция для вычисления площади круга по радиусу.
    """
    if radius > 0:
        return math.pi * radius ** 2
    else:
        return "Ошибка: радиус должен быть положительным числом"


def triangle_area(side1, side2, side3):
    """
    Функция для вычисления площади треугольника по трем сторонам.
    """
    if side1 > 0 and side2 > 0 and side3 > 0:
        # Проверяем, является ли треугольник прямоугольным
        if (side1 ** 2 + side2 ** 2 == side3 ** 2) or (side1 ** 2 + side3 ** 2 == side2 ** 2) or (
                side2 ** 2 + side3 ** 2 == side1 ** 2):
            # Если треугольник прямоугольный, используем формулу площади для прямоугольного треугольника
            return (side1 * side2) / 2
        else:
            # Иначе, используем формулу для вычисления площади треугольника
            semi_perimeter = (side1 + side2 + side3) / 2
            return math.sqrt(
                semi_perimeter * (semi_perimeter - side1) * (semi_perimeter - side2) * (semi_perimeter - side3))
    else:
        return "Ошибка: все стороны треугольника должны быть положительными числами"


# Задание 2:
# В датафреймах (pyspark.sql.DataFrame) заданы продукты, категории и связь между ними. Одному продукту может
# соответствовать много категорий, в одной категории может быть много продуктов. Напишите метод с помощью PySpark,
# который вернет все продукты с их категориями (датафрейм с набором всех пар «Имя продукта – Имя категории»).
# В результирующем датафрейме должны также присутствовать продукты, у которых нет категорий.


# Импортируем модуль pyspark (служит для упрощения работы с данными и позволяет выполнять запросы на языке SQL)
from pyspark.sql import SparkSession

# Создаем объект SparkSession
spark = SparkSession.builder.getOrCreate()

# Создаем таблицу продуктов и категорий
products = spark.createDataFrame([
    ('product1', 'category1'),
    ('product2', 'category2'),
    ('product3', 'category1'),
    ('product4', None)
], ['product_name', 'category_name'])

categories = spark.createDataFrame([
    ('category1', 'Category 1'),
    ('category2', 'Category 2')
], ['category_name', 'category_description'])

# Выполняем объединение табл
result = products.join(categories, products.category_name == categories.category_name, how='left')

# Выводим результат
result.show()