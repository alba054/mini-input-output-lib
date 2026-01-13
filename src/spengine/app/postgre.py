from functools import wraps
import re
import traceback
from geoalchemy2 import Geography, Geometry, WKTElement


from loguru import logger

import psycopg2
from sqlalchemy import create_engine, func, inspect, text
from sqlalchemy.dialects.postgresql import insert

import pandas as pd

from spengine.helper.helper import generate_id_str

from sqlalchemy.exc import (
    PendingRollbackError,
    DBAPIError,
    OperationalError,
    DisconnectionError,
)


class PgSaService:
    _instances = dict()

    def __init__(self, username, password, host, port, db):
        self._host = host
        self._port = port
        self._db = db
        self._username = username
        self._password = password
        self._hash = generate_id_str(f"{host}{port}{db}")
        self.connect = create_engine("postgresql+psycopg2://", creator=self.connection).connect()

    def reconnect_handler_wrapper(fn):
        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            try:
                return fn(self, *args, **kwargs)

            except (PendingRollbackError, DisconnectionError, OperationalError, DBAPIError):
                print("reconnecting...")
                self.connect = self.connection()
                return fn(self, *args, **kwargs)

        return wrapper

    def __call__(cls, username, password, host, port, db):
        hash = generate_id_str(f"{host}{port}{db}")
        if hash not in cls._instances.keys():
            print("doesn't exist, create singleton object")
            instance = super().__call__(username, password, host, port, db)
            cls._instances[hash] = instance
        return cls._instances[hash]

    def insert_on_conflict_nothing(self, table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_statement = insert(table.table).values(data)
        conflict_update = insert_statement.on_conflict_do_update(
            constraint=f"{table.table.name}_pkey",
            set_={column.key: column for column in insert_statement.excluded},
        )
        result = conn.execute(conflict_update)
        return result.rowcount

    def insert_on_conflict_nothing_custom_set(self, table, conn, keys, data_iter, exclude_cols=None):
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_statement = insert(table.table).values(data)

        if exclude_cols:
            # Update only the specified columns
            conflict_update = insert_statement.on_conflict_do_update(
                constraint=f"{table.table.name}_pkey",
                set_={
                    col: insert_statement.excluded[col]
                    for col in insert_statement.excluded.keys()
                    if col not in exclude_cols
                },
            )
        else:
            # Update all columns by default
            conflict_update = insert_statement.on_conflict_do_update(
                constraint=f"{table.table.name}_pkey",
                set_={column.key: column for column in insert_statement.excluded},
            )

        result = conn.execute(conflict_update)
        return result.rowcount

    def insert_on_conflict_do_nothing(self, table, conn, keys, data_iter):
        """
        Insert data into a table, ignoring rows that violate primary key constraints.
        """
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_statement = insert(table.table).values(data)
        conflict_ignore = insert_statement.on_conflict_do_nothing()
        result = conn.execute(conflict_ignore)
        return result.rowcount

    def insert_on_conflict_do_nothing_with_geom(self, table, conn, keys, data_iter):
        """
        Execute a bulk insert operation that ignores conflicts,
        with proper handling of WKTElement geometry objects.

        Args:
            table: pandas.io.sql.SQLTable object
            conn: SQLAlchemy connection object
            keys: List of column names
            data_iter: Iterator yielding rows to be inserted

        Returns:
            int: Number of rows actually inserted (excluding conflicts)
        """
        # Get the SQLAlchemy table object from pandas SQLTable
        sa_table = table.table
        insp = inspect(conn.engine)

        # Identify geometry columns
        geom_columns = {}
        for col in insp.get_columns(sa_table.name, schema=sa_table.schema):
            if isinstance(col["type"], (Geometry, Geography)):
                geom_columns[col["name"]] = {
                    "srid": col["type"].srid,
                    "geometry_type": getattr(col["type"], "geometry_type", None),
                }

        def convert_coordinate_string(coord_str, srid):
            """Convert coordinate string to WKTElement"""
            try:
                if isinstance(coord_str, str):
                    if "," in coord_str:
                        # Handle "lat,lon" format
                        lat, lon = map(float, re.split(r"[,;]\s*", coord_str.strip()))
                        return f"POINT({lon} {lat})"
                    elif " " in coord_str:
                        # Handle "lon lat" format
                        lon, lat = map(float, coord_str.strip().split())
                        return f"POINT({lon} {lat})"
                    elif coord_str.upper().startswith(("POINT", "LINESTRING", "POLYGON")):
                        # Already in WKT format
                        return coord_str
            except (ValueError, AttributeError) as e:
                logger.warning(f"Coordinate conversion error: {e}")
            return coord_str

        def process_row(row):
            row_dict = dict(zip(keys, row))
            for col_name, col_info in geom_columns.items():
                if col_name in row_dict and row_dict[col_name] is not None:
                    # Convert coordinate strings to WKT format
                    if isinstance(row_dict[col_name], str):
                        row_dict[col_name] = convert_coordinate_string(row_dict[col_name], col_info["srid"])

                    # Convert to text representation if it's a WKTElement
                    if isinstance(row_dict[col_name], WKTElement):
                        row_dict[col_name] = row_dict[col_name].desc

                    # Add SRID if missing from WKT string
                    if isinstance(row_dict[col_name], str) and "SRID" not in row_dict[col_name] and col_info["srid"]:
                        row_dict[col_name] = f"SRID={col_info['srid']};{row_dict[col_name]}"
            return row_dict

        # Process all rows
        data = [process_row(row) for row in data_iter]

        if not data:
            return 0

        # Prepare the insert statement using text representations
        stmt = insert(sa_table).values(data)
        conflict_stmt = stmt.on_conflict_do_nothing()

        try:
            result = conn.execute(conflict_stmt)
            return result.rowcount
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}. Falling back to row-by-row insertion.")

            inserted_rows = 0
            for row in data:
                try:
                    # Use ST_GeomFromEWKT for proper geometry handling
                    geom_updates = {}
                    for col_name in geom_columns:
                        if col_name in row and row[col_name]:
                            geom_updates[col_name] = func.ST_GeomFromEWKT(row[col_name])

                    # Create update dictionary with geometry conversions
                    update_values = {k: v for k, v in row.items() if k not in geom_columns}
                    update_values.update(geom_updates)

                    single_stmt = insert(sa_table).values(update_values).on_conflict_do_nothing()
                    result = conn.execute(single_stmt)
                    inserted_rows += result.rowcount
                except Exception as single_row_error:
                    logger.warning(f"Failed to insert row: {single_row_error}\n" f"Row data: {row}")
                    continue

            return inserted_rows

    def connection(self):
        return psycopg2.connect(
            f"dbname={self._db} user={self._username} password={self._password} host={self._host} port={self._port}"
        )

    @reconnect_handler_wrapper
    def ingest(self, data: list, chunk_size, tb_name, schema=None, exclude_cols=None):
        try:
            df = pd.DataFrame(data)
            # if len(df) > 1:
            #     df = df.drop_duplicates()
            total_data = len(df)
            connect = self.connect

            # connect = self.connection()
            # with self.connect.begin() as connect:
            if schema:
                df.to_sql(
                    tb_name,
                    con=connect,
                    chunksize=chunk_size,
                    if_exists="append",
                    index=False,
                    schema=schema,
                    method=lambda table, conn, keys, data_iter: self.insert_on_conflict_nothing_custom_set(
                        table, conn, keys, data_iter, exclude_cols=exclude_cols
                    ),
                )
            else:
                df.to_sql(
                    tb_name,
                    con=connect,
                    chunksize=chunk_size,
                    if_exists="append",
                    index=False,
                    method=lambda table, conn, keys, data_iter: self.insert_on_conflict_nothing_custom_set(
                        table, conn, keys, data_iter, exclude_cols=exclude_cols
                    ),
                )

            # connect.close()
            # connect.commit()
            logger.info(f"Total Data Insert = {total_data} || {tb_name}")
            return total_data
        except psycopg2.OperationalError as e:
            self.connect = create_engine("postgresql+psycopg2://", creator=self.connection).connect()
            self.ingest(data, chunk_size, tb_name, schema, exclude_cols)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            # connect.rollback()
            # connect.close()
            return 0

    @reconnect_handler_wrapper
    def update_rows(
        self,
        table: str,
        update_values: dict,
        schema: str = "public",
        condition: str = "",
        condition_values: dict = dict(),
    ):
        try:
            connection = self.connect

            placeholders = []

            for k in update_values.keys():
                placeholders.append(f"{k} = :{k}")

            connection.execute(
                text(
                    f"UPDATE {schema}.{table} SET {','.join(placeholders)} {'WHERE ' + condition if condition != '' else ''}"
                ),
                {**update_values, **condition_values},
            )

            connection.commit()
        except psycopg2.OperationalError as e:
            self.connect = create_engine("postgresql+psycopg2://", creator=self.connection).connect()
            self.update_rows(table, update_values, schema, condition, condition_values)
        except Exception as e:
            logger.error(f"Failed to update rows: {e}")
            connection.rollback()
            return 0

    def ingest_with_geom(self, data: list, chunk_size, tb_name, schema=None):
        try:
            df = pd.DataFrame(data)
            total_data = len(df)
            connect = self.connect

            # connect = self.connection()
            if schema:
                df.to_sql(
                    tb_name,
                    con=connect,
                    chunksize=chunk_size,
                    if_exists="append",
                    index=False,
                    schema=schema,
                    method=self.insert_on_conflict_do_nothing_with_geom,
                )
            else:
                df.to_sql(
                    tb_name,
                    con=connect,
                    chunksize=chunk_size,
                    if_exists="append",
                    index=False,
                    method=self.insert_on_conflict_do_nothing_with_geom,
                )

            # connect.close()
            connect.commit()
            logger.info(f"Total Data Insert = {total_data} || {tb_name}")
            return total_data
        except Exception as e:
            logger.error(e)
            connect.rollback()
            # connect.close()
            return 0

    def ingest2(self, df, chunk_size, tb_name, schema, conflict_method=None):
        """
        Insert data into the database, handling conflicts if specified.
        """
        try:
            total_data = len(df)
            connect = self.connection()

            # Pilih metode penyisipan
            insert_method = conflict_method or self.insert_on_conflict_do_nothing

            # Gunakan DataFrame `to_sql` untuk batch insert
            df.to_sql(
                tb_name,
                con=connect,
                chunksize=chunk_size,
                if_exists="append",
                index=False,
                schema=schema,
                method=insert_method,
            )
            connect.close()
            return total_data
        except Exception as e:
            logger.error(e)
            connect.rollback()
            connect.close()
            return 0

    # async def ingest2(self, df, chunk_size, tb_name, schema, conflict_method=None):
    #     """
    #     Insert data into the database, handling conflicts if specified.
    #     """
    #     try:
    #         total_data = len(df)

    #         # Mendapatkan session asinkron dengan memanggil get_session
    #         async with self.async_session_factory() as session:
    #             async with session.begin():
    #                 for i in range(0, total_data, chunk_size):
    #                     chunk = df.iloc[i:i + chunk_size]

    #                     stmt = insert(f"{schema}.{tb_name}").values(chunk.to_dict(orient='records'))

    #                     # Menjalankan query secara asinkron
    #                     if conflict_method:
    #                         await conflict_method(session, stmt, chunk.to_dict(orient='records'))
    #                     else:
    #                         # Jika tidak ada method konflik, jalankan insert standar
    #                         await session.execute(stmt)

    #                     # Commit setelah eksekusi
    #                     await session.commit()

    #         return total_data
    #     except Exception as e:
    #         logger.error(f"Error during ingest: {e}")
    #         await session.rollback()  # Jika error, rollback
    #         return 0

    def insert_not_primary(self, df, chunk_size, tb_name):
        try:
            total_data = len(df)
            connect = self.connection()
            df.to_sql(
                tb_name,
                con=connect,
                chunksize=chunk_size,
                if_exists="append",
                index=False,
            )
            connect.close()
            return total_data
        except Exception as e:
            logger.error(e)
            connect.rollback()
            connect.close()
            return 0

    @reconnect_handler_wrapper
    def read_data(self, query: str):
        try:
            connect = self.connect
            result_df = pd.read_sql(query, con=connect)
            # connect.close()
            return result_df
        except Exception as e:
            logger.error(e)
            # connect.close()
            return None
