import os
import pyodbc
from dotenv import load_dotenv
import SQL_Queries
import json


class ConnectDB:
    @staticmethod
    def connect_to_db(server, database, user, password):
        connectionString = f'''DRIVER={{ODBC Driver 18 for SQL Server}};
                               SERVER={server};
                               DATABASE={database};
                               UID={user};
                               PWD={password};
                               Encrypt=Optional;'''
        try:
            conn = pyodbc.connect(connectionString)
            conn.autocommit = True
        except pyodbc.ProgrammingError as ex:
            print(f"Ошибка: {ex}")
        else:
            return conn


class MSSQLOperator:
    def __init__(self, connector_obj):
        self.conn = connector_obj

    def select_query(self, database_name, selected: tuple, table: str, additions: str = None, union: str = None):
        cursor = self.conn.cursor()
        cursor.execute(f"USE {database_name};")
        command = SQL_Queries.select_query_gen(selected, table, additions, union)
        print(command)
        try:
            result = cursor.execute(command)
        except pyodbc.ProgrammingError as ex:
            print(ex)
        else:
            return [dict(zip([column[0] for column in result.description], row)) for row in result.fetchall()]
        finally:
            cursor.close()

    @staticmethod
    def get_select_query(database_name, selected: tuple, table: str, additions: str = None, union: str = None):
        command = SQL_Queries.select_query_gen(selected, table, additions, union)
        return command

    @staticmethod
    def load_data_to_json(filename, data):
        with open(f"../jsons/{filename}.json", 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        return f"Создан {filename}.json"

    def create_database(self, database_name):
        SQL_COMMAND = SQL_Queries.create_db(database_name)
        try:
            self.conn.execute(SQL_COMMAND)
        except pyodbc.ProgrammingError as ex:
            print(ex)
        else:
            return f"База данных: {database_name} создана"

    def create_table(self, database_name, table_name, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f'USE {database_name}')
        SQL_Query = sql_query(table_name)
        try:
            cursor.execute(SQL_Query)
        except pyodbc.ProgrammingError as ex:
            return ex
        else:
            return f"Таблица: {table_name} создана"

    @staticmethod
    def get_data_from_json(filename):
        with open(filename, 'r', encoding='utf-8') as file:
            python_data = json.load(file)
            return python_data

    def fill_table(self, database_name, table_name, filename, sql_query):
        cursor = self.conn.cursor()
        cursor.execute(f"USE {database_name}")
        data_to_fill_list = self.get_data_from_json(filename)
        try:
            for data_to_fill in data_to_fill_list:
                cursor.execute(sql_query(table_name, data_to_fill))
        except pyodbc.Error as ex:
            return ex
        else:
            return "Данные помещены в таблицу"


if __name__ == "__main__":
    load_dotenv()
    SERVER = os.getenv('MS_SQL_SERVER')
    DATABASE = os.getenv('MS_SQL_DATABASE')
    USER = os.getenv('MS_SQL_USER')
    PASSWORD = os.getenv('MS_SQL_KEY')
    db_operator = MSSQLOperator(ConnectDB.connect_to_db(SERVER, DATABASE, USER, PASSWORD))


    def create_queries():
        print(db_operator.create_database("TestDB"))
        print(db_operator.create_table("TestDB", "test_table", SQL_Queries.create_test))
        file_path = r"../jsons/test_data.json"
        print(db_operator.fill_table("TestDB", "test_table", file_path, SQL_Queries.fill_test))
        result = db_operator.select_query("TestDB", ("*",), "test_table")
        print(db_operator.load_data_to_json("test_table_request", result))

    def request_queries():
        # два запроса Exists;

        condition = ("WHERE EXISTS (SELECT * FROM Instructors AS i, Sections  AS s "
                     "WHERE i.id = Visitors.instructor_id "
                     "AND i.section_id = s.id "
                     "AND s.name <> 'Йога')")
        result = db_operator.select_query("Sections", ("firstname", "id"), "Visitors", condition)
        print(db_operator.load_data_to_json("exists_1", result))

        condition = ("WHERE EXISTS (SELECT * FROM Instructors AS i, Sections AS s "
                     "WHERE i.id = Visitors.instructor_id "
                     "AND i.section_id = s.id "
                     "AND s.name = 'Силовые тренировки')")
        result = db_operator.select_query("Sections", ("firstname", "id"), "Visitors", condition)
        print(db_operator.load_data_to_json("exists_2", result))

        # по одному запросу на ANY/SOME;

        condition = "WHERE id = ANY(SELECT id FROM Visitors WHERE DATEPART(DAY, visit_date) > 3)"
        result = db_operator.select_query("Sections", ("firstname", "id"), "Visitors", condition)
        print(db_operator.load_data_to_json("full_join", result))

        condition = "WHERE section_id = SOME (SELECT id FROM Sections WHERE name = 'Кардио тренировки')"
        result = db_operator.select_query("Sections", ("firstname", "id"), "Instructors", condition)
        print(db_operator.load_data_to_json("full_join", result))

        # один запрос ALL;

        condition = "WHERE attendance >= ALL (SELECT attendance FROM Visitors)"
        result = db_operator.select_query("Sections", ("firstname", "id"), "Visitors", condition)
        print(db_operator.load_data_to_json("all", result))

        # один запрос на UNION;

        condition_1 = "WHERE visit_date BETWEEN '2024-01-01' AND '2024-03-31'"
        condition_2 = "WHERE visit_date BETWEEN '2024-04-01' AND '2024-06-30'"
        union_query = db_operator.get_select_query("Sections", ("firstname", "id"), "Visitors", condition_1)
        result = db_operator.select_query("Sections", ("firstname", "id"), "Visitors", condition_2, union_query)
        print(db_operator.load_data_to_json("union", result))

        # один запрос на UNION ALL;

        condition_1 = "WHERE section_id = 1"
        condition_2 = "WHERE section_id = 3"
        union_query = f"ALL {db_operator.get_select_query("Sections", ("firstname", "id"), "Instructors", condition_1)}"
        result = db_operator.select_query("Sections", ("firstname", "id"), "Instructors", condition_2, union_query)
        print(db_operator.load_data_to_json("union_all", result))

        # по одному запросу на все JOIN (INNER/LEFT/RIGHT/LEFT+RIGHT/FULL) - всего пять штук;

        join = "INNER JOIN Instructors i ON s.id = i.section_id"
        result = db_operator.select_query("Sections", ("s.id", "s.name", "i.id", "i.firstname"), "Sections s", join)
        print(db_operator.load_data_to_json("inner", result))

        join = "LEFT JOIN Sections s ON v.instructor_id = s.id"
        result = db_operator.select_query("Sections", ("v.id", "v.firstname", "s.id", "s.name"), "Visitors v", join)
        print(db_operator.load_data_to_json("left", result))

        join = "RIGHT JOIN Sections s ON v.instructor_id = s.id"
        result = db_operator.select_query("Sections", ("v.id", "v.firstname", "s.id", "s.name"), "Visitors v", join)
        print(db_operator.load_data_to_json("right", result))

        join_1 = "LEFT JOIN Sections s ON v.instructor_id = s.id"
        join_2 = "RIGHT JOIN Sections s ON v.instructor_id = s.id"
        union_query = f"{db_operator.get_select_query("Sections", ("v.id", "v.firstname", "s.id", "s.name"), "Visitors v",
                                                      join_1)}"
        result = db_operator.select_query("Sections", ("v.id", "v.firstname", "s.id", "s.name"), "Visitors v", join_2,
                                          union_query)
        print(db_operator.load_data_to_json("left_right", result))

        join = "FULL JOIN Sections s ON v.instructor_id = s.id"
        result = db_operator.select_query("Sections", ("v.id", "v.firstname", "s.id AS section_id", "s.name"),
                                          "Visitors v", join)
        print(db_operator.load_data_to_json("full", result))

    # request_queries()
    create_queries()