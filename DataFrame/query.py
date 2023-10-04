from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

products = spark.createDataFrame(
    [
        (1, "Компьютер"),
        (2, "Футболка"),
        (3, "Смартфон"),
        (4, "Шорты"),
        (5, "Домкрат"),
        (6, "Компрессор"),
        (7, "Навигатор"),
        (8, "Стул"),
    ],
    ["product_id", "product_name"]
)

categories = spark.createDataFrame(
    [
        (1, "Электроника"),
        (2, "Одежда"),
        (3, "Автотовары"),
    ],
    ["category_id", "category_name"]
)

product_category = spark.createDataFrame(
    [
        (1, 1),
        (2, 2),
        (3, 1),
        (4, 2),
        (5, 3),
        (6, 3),
        (7, 1),
        (7, 3),
    ],
    ["product_id", "category_id"]
)

result = (products.join(product_category, "product_id", "left")
          .join(categories, "category_id", "left")
          .select("product_name", "category_name"))

result.show()
