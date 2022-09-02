from typing import Dict, Any, List, Iterable

from psycopg2 import sql
from psycopg2._psycopg import connection
from psycopg2.extras import execute_values, RealDictCursor

from gainy.data_access.models import BaseModel

MAX_TRANSACTION_SIZE = 100


class TableFilter:

    @staticmethod
    def _where_clause_statement(filter_by: Dict[str, Any]):
        condition = sql.SQL(" AND ").join([
            sql.SQL(f"{{field}} = %({field})s").format(
                field=sql.Identifier(field)) for field in filter_by.keys()
        ])
        return sql.SQL(" WHERE ") + condition


class TableLoad(TableFilter):

    def find_one(self, cls, filter_by: Dict[str, Any] = None):
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(self._filter_query(cls, filter_by), filter_by)

            row = cursor.fetchone()

        return cls(row) if row else None

    def iterate_all(self,
                    cls,
                    filter_by: Dict[str, Any] = None) -> Iterable[Any]:
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(self._filter_query(cls, filter_by), filter_by)

            for row in cursor:
                yield cls(row)

    def find_all(self, cls, filter_by: Dict[str, Any] = None) -> List[Any]:
        return list(self.iterate_all(cls, filter_by))

    def _filter_query(self, cls, filter_by):
        query = sql.SQL("SELECT * FROM {schema_name}.{table_name}").format(
            schema_name=sql.Identifier(cls.schema_name),
            table_name=sql.Identifier(cls.table_name))

        if filter_by:
            query += self._where_clause_statement(filter_by)

        return query


class TableDelete(TableFilter):

    def delete(self, cls, filter_by: Dict[str, Any]):
        query = sql.SQL("DELETE FROM {schema_name}.{table_name}").format(
            schema_name=sql.Identifier(cls.schema_name),
            table_name=sql.Identifier(cls.table_name))

        query += self._where_clause_statement(filter_by)

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, filter_by)


class TablePersist:

    def commit(self):
        self.db_conn.commit()

    def persist(self, entities):
        if isinstance(entities, BaseModel):
            entities = [entities]

        entities_grouped = self._group_entities(entities)

        for (schema_name,
             table_name), group_entities in entities_grouped.items():

            chunks_count = (len(group_entities) + MAX_TRANSACTION_SIZE -
                            1) // MAX_TRANSACTION_SIZE
            for chunk_id in range(chunks_count):

                l_bound = chunk_id * MAX_TRANSACTION_SIZE
                r_bound = min(len(group_entities),
                              (chunk_id + 1) * MAX_TRANSACTION_SIZE)
                chunk = group_entities[l_bound:r_bound]

                self._persist_chunk(schema_name, table_name, chunk)

    def _persist_chunk(self, schema_name, table_name, entities):
        field_names = [
            field_name for field_name in entities[0].to_dict().keys()
            if field_name not in entities[0].db_excluded_fields
        ]
        non_persistent_fields = entities[0].non_persistent_fields

        sql_string = self._get_insert_statement(schema_name, table_name,
                                                field_names, entities)

        entity_dicts = [entity.to_dict() for entity in entities]
        values = [[entity_dict[field_name] for field_name in field_names]
                  for entity_dict in entity_dicts]

        with self.db_conn.cursor() as cursor:
            execute_values(cursor, sql_string, values)

            if not non_persistent_fields:
                return

            returned = cursor.fetchall()

        for entity, returned_row in zip(entities, returned):
            for non_persistent_field, value in zip(non_persistent_fields,
                                                   returned_row):
                entity.__setattr__(non_persistent_field, value)

    def _get_insert_statement(self, schema_name, table_name, field_names,
                              entities):
        field_names_escaped = self._escape_fields(field_names)

        sql_string = sql.SQL(
            "INSERT INTO {schema_name}.{table_name} ({field_names}) VALUES %s"
        ).format(schema_name=sql.Identifier(schema_name),
                 table_name=sql.Identifier(table_name),
                 field_names=field_names_escaped)

        key_fields = entities[0].key_fields
        if key_fields:
            key_field_names_escaped = self._escape_fields(key_fields)

            sql_string = sql_string + sql.SQL(
                " ON CONFLICT({key_field_names}) DO UPDATE SET {set_clause}"
            ).format(
                key_field_names=key_field_names_escaped,
                set_clause=sql.SQL(',').join([
                    sql.SQL("{field_name} = excluded.{field_name}").format(
                        field_name=sql.Identifier(field_name))
                    for field_name in field_names
                    if field_name not in key_fields
                ]))

        non_persistent_fields = entities[0].non_persistent_fields
        if non_persistent_fields:
            non_persistent_fields_escaped = self._escape_fields(
                non_persistent_fields)
            sql_string = sql_string + sql.SQL(
                " RETURNING {non_persistent_fields}").format(
                    non_persistent_fields=non_persistent_fields_escaped)
        return sql_string

    @staticmethod
    def _escape_fields(field_names):
        field_names_escaped = sql.SQL(',').join(
            map(sql.Identifier, field_names))
        return field_names_escaped

    def _group_entities(self, entities):
        entities_grouped = {}

        for entity in entities:
            key = (entity.schema_name, entity.table_name)
            if key in entities_grouped:
                entities_grouped[key].append(entity)
            else:
                entities_grouped[key] = [entity]

        return entities_grouped


class Repository(TableLoad, TablePersist, TableDelete):

    def __init__(self, db_conn):
        self.db_conn = db_conn
