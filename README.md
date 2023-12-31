# Отчёт к лабораторной работе

## Запуск

1. Если нужно, установите Python 3, а после необходимые библиотеки с помощью команды в терминале:

```bash
pip install psycopg2-binary duckdb sqlalchemy pandas 
```
2. Клонируйте репозиторий с помощью команды:

```bash
git clone https://github.com/Sherukas/lab3_benchmark.git
```
3. В конфигурационном файле для каждой библиотеки выберите, нужно ли её использоать. А также, нужно ли для неё заново загрузить данные или нет (если данные уже загружены, в PostgreSQL или программа запускается не первый раз, и уже создан файл .db, то выберите не загружать данные).
Также  выберите количество тестов для каждого запроса и путь, по которому лежит csv файл с данными.
Если используется библиотека psycopg2 или sqlalchemy, то не забудьте запустить PostgreSQL.

4. Запустите файл lab_3.py:

Результаты тестов будут сохранены в паке results в файле res.csv

## Результаты

![table](https://github.com/Sherukas/lab3_benchmark/blob/main/graphics/table.png)

![graph1](https://github.com/Sherukas/lab3_benchmark/blob/main/graphics/graph1.png)

![graph2](https://github.com/Sherukas/lab3_benchmark/blob/main/graphics/graph2.png)

## Отчёт

## 1. Psycopg2 (PostgreSQL):
Psycopg2 - библиотека для работы с базой данных PostgreSQL. Она обладает средней скоростью выполнения запросов. На всех запросах показала скорость хуже, чем почти у всех остальных библиотек. Исползовать библиотеку не слишком сложно, но нужно отдельно подключаться к базе данных PostgreSQL, что доставляет неудобств. 
Скорость: Ниже среднего.
Простота в использовании: Средняя.

## 2. SQLite:
SQLite - это легковесная библиотека для работы с базой данных SQLite. Она довольно проста виспользовании и не требует много лишних знаний для работы с ней, а небольшой объём кода обеспечивает высокую скорость работы. Благодаря своей легковесности, она хорошо подходит для использования встроенной базы данных. Показала довольно хорошую скорость по сравнению с Psycopg2 и SQLAlchemy, хотя и уступает другим.
Скорость: Средняя.
Простота в использовании: Выше среднего.

## 3. DuckDB:
DuckDB - это очень быстрая и эффективная библиотека для работы с базой данных, которая позволяет проводить операции с данными на лету. Работает очень быстро, потому что оптимизирована для работы с большими объемами данных и предлагает высокую скорость выполнения запросов. Однако, у нее ограниченная поддержка SQL функций, что впрочем не мешает ей выполнять большинство запросов. Не требует подключения чего-либо дополнительно и довольно проста в использовании.
По моему мнению, самая удобная библиотека.
Скорость: Очень высокая.
Простота в использовании: Выше среднего.

## 4. SQLAlchemy:
SQLAlchemy - это мощная и гибкая библиотека для работы с различными базами данных. Предоставляет высокий уровень абстракции, позволяя писать запросы, используя код питона, а таблицы представлять в виде объектов. Но из-за этого сильно возрастает сложность в использовании. Но SQLAlchemy также имеет возможности поддержки SQL-запросов и транзакций. Однако, из-за своей гибкости и уровня абстракции, библиотека медленнее в сравнении с низкоуровневыми библиотеками.
Скорость: Ниже среднего.
Простота в использовании: Средняя.

## 5. Pandas:
Впечатления: Pandas - это библиотека для обработки и анализа данных, которая также предоставляет возможность работать с базой данных через встроенный интерфейс. Она имеет широкий спектр возможностей. И при этом показывает себя довольно быстро в сравнении с другими библиотеками. Довольно легка в использовании, позволяет избежать много трудностей с подключением базы данных.
Скорость: Выше среднего.
Простота в использовании: Выше среднего.

## Выводы
Таким образом, если важна скорость выполнения запросов, то лучше выбрать DuckDB или Pandas. Если простота использования имеет первостепенное значение, то SQLite и Pandas предоставляют более простой интерфейс. В целом, DuckDB и Pandas хороши как по скорости, так и по простоте использования. 
