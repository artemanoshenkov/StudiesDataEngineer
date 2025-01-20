
import os
import random
import datetime
from pyspark.sql import SparkSession

null_values = 0.05
names = []
cities = []

with open('names.txt', 'r') as file:
    for line in file.readlines():
        name = line.split()[1].strip()
        if len(name) >= 5:
            names.append(name)

with open('cities.txt', 'r') as file:
    for line in file.readlines():
        cities.append(line.split()[1].strip())


class FakePerson():

    def __init__(self, id):
        self.id = id
        self.name = random.choice(names)
        self.email = self.name + '@' + random.choice(('yandex.ru', 'gmail.com', 'mail.ru'))
        self.city = random.choice(cities)
        self.age = random.choice(range(18, 96))
        self.salary = random.choice(range(50000, 500000))
        self.registration_date = self.fake_registration_date(self.age)

    def fake_registration_date(self, age):
        birth_year = datetime.date.today().year - age
        birth_date = datetime.datetime.strptime(f'{birth_year}-01-01', '%Y-%m-%d') + datetime.timedelta(days=random.choice(range(365)))
        birth_date = birth_date.date()
        registration_date = birth_date + datetime.timedelta(days=random.randint(0, (datetime.date.today() - birth_date).days))
        return registration_date


    def info(self):
        return (self.id,
                self.name,
                self.email,
                self.city,
                self.age,
                self.salary,
                self.registration_date)

def god_of_null(input):
    return tuple(None if random.random() < null_values else item for item in input)

def refactoring_dir(dirname):
    for filename in os.listdir(f'{dirname}/'):
        if filename.startswith('part'):
            os.rename(f'{dirname}/{filename}', f'{dirname}/{datetime.date.today()}-dev.scv')
        elif filename == '_SUCCESS':
            os.remove(f'{dirname}/{filename}')
    print(f'Данные сохранены в {dirname}/{datetime.date.today()}-dev.scv')

number_of_rows = int(input('Введите число генерируемых данных (количество строк): '))

spark = SparkSession.builder\
    .appName("generator")\
    .getOrCreate()

if number_of_rows // (1 - null_values) > number_of_rows:
    data = [god_of_null(FakePerson(i).info()) for i in range(1, number_of_rows + 1)]
else:
    data = [FakePerson(i).info() for i in range(1, number_of_rows + 1)]

df = spark.createDataFrame(data, ['id', 'name', 'email', 'city', 'age', 'salary', 'registration_date'])

df.repartition(1).write.csv(f'content/', header=True, mode='overwrite')
refactoring_dir('content')

spark.stop()