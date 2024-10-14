def select_query_gen(selected: tuple, table: str, additions: str, union: str):
    if union:
        union = f"UNION {union}"
    else:
        union = ''
    COMMAND = (fr"SELECT {", ".join(selected)} "
               fr"FROM {table} "
               fr"{additions} "
               fr"{union}")
    return COMMAND
