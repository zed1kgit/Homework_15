def select_query_gen(selected: tuple, table: str, additions: str, union: str):
    if union:
        union = f"UNION {union}"
    else:
        union = ''
    if not additions:
        additions = ''
    COMMAND = (fr"SELECT {", ".join(selected)} "
               fr"FROM {table} "
               fr"{additions} "
               fr"{union}")
    return COMMAND


def create_db(name):
    COMMAND = fr"CREATE DATABASE {name}"
    return COMMAND


def create_test(table_name):
    COMMAND = fr"""CREATE TABLE {table_name}
            (id int PRIMARY KEY IDENTITY(1, 1),
            test_name nvarchar(100))"""
    return COMMAND


def fill_test(table_name, data_to_fill):
    COMMAND = fr"""INSERT INTO {table_name} (test_name)
                        VALUES
                        ('{data_to_fill['test_name']}')"""
    return COMMAND
