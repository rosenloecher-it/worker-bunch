import logging
import os
import pathlib
import sys
from typing import List, Optional

import psycopg
# noinspection PyPackageRequirements
import testing.postgresql
from psycopg.rows import dict_row


class SetupTestException(Exception):
    pass


_logger = logging.getLogger(__name__)


# noinspection SpellCheckingInspection
class SetupTest:

    TEST_DIR = "__test__"
    DATABASE_DIR = os.path.join(TEST_DIR, "database")

    _logging_inited = False
    _postgresql: Optional[testing.postgresql.Postgresql] = None

    @classmethod
    def init_logging(cls):
        if not cls._logging_inited:
            cls._logging_inited = True

            logging.basicConfig(
                format='[%(levelname)8s] %(name)s: %(message)s',
                level=logging.DEBUG,
                handlers=[logging.StreamHandler(sys.stdout)]
            )

            logging.getLogger("asyncio").setLevel(logging.WARNING)

    @classmethod
    def get_project_dir(cls) -> str:
        file_path = os.path.dirname(__file__)
        out = os.path.dirname(file_path)  # go up one time
        return out

    @classmethod
    def get_test_dir(cls) -> str:
        project_dir = cls.get_project_dir()
        out = os.path.join(project_dir, cls.TEST_DIR)
        return out

    @classmethod
    def get_test_path(cls, relative_path) -> str:
        return os.path.join(cls.get_test_dir(), relative_path)

    @classmethod
    def get_database_dir(cls) -> str:
        project_dir = cls.get_project_dir()
        out = os.path.join(project_dir, cls.DATABASE_DIR)
        return out

    @classmethod
    def ensure_test_dir(cls) -> str:
        return cls.ensure_dir(cls.get_test_dir())

    @classmethod
    def ensure_clean_test_dir(cls) -> str:
        return cls.ensure_clean_dir(cls.get_test_dir())

    @classmethod
    def ensure_database_dir(cls) -> str:
        return cls.ensure_dir(cls.get_database_dir())

    @classmethod
    def ensure_clean_database_dir(cls) -> str:
        return cls.ensure_clean_dir(cls.get_database_dir())

    @classmethod
    def ensure_dir(cls, dirpath) -> str:
        exists = os.path.exists(dirpath)

        if exists and not os.path.isdir(dirpath):
            raise NotADirectoryError(dirpath)
        if not exists:
            os.makedirs(dirpath)

        return dirpath

    @classmethod
    def ensure_clean_dir(cls, dirpath) -> str:
        if not os.path.exists(dirpath):
            cls.ensure_dir(dirpath)
        else:
            cls.clean_dir_recursively(dirpath)

        return dirpath

    @classmethod
    def clean_dir_recursively(cls, path_in):
        dir_segments = pathlib.Path(path_in)
        if not dir_segments.is_dir():
            return
        for item in dir_segments.iterdir():
            if item.is_dir():
                cls.clean_dir_recursively(item)
                os.rmdir(item)
            else:
                item.unlink()

    @classmethod
    def init_database(cls, recreate=False):
        database_dir = cls.get_database_dir()

        if recreate:
            cls.close_database(shutdown=True)

        if not cls._postgresql:
            cls.ensure_clean_dir(database_dir)
            cls._postgresql = testing.postgresql.Postgresql(base_dir=database_dir)

    @classmethod
    def close_database(cls, shutdown=False):
        if cls._postgresql:
            if shutdown:
                cls._postgresql.stop()
                cls._postgresql = None

    @classmethod
    def get_database_params(cls, psycopg_naming=False):
        if cls._postgresql:
            params = cls._postgresql.dsn()

            if psycopg_naming:
                db_name_1 = params.get("dbname")
                db_name_2 = params.get("database")
                if not db_name_1 and db_name_2:
                    params["dbname"] = db_name_2
                    del params["database"]

            return params
        else:
            return {}

    @classmethod
    def execute_commands(cls, commands: List[str]):
        if not cls._postgresql:
            raise SetupTestException("Database not initialized!")

        with psycopg.connect(**SetupTest.get_database_params(psycopg_naming=True)) as connection:
            with connection.cursor() as cursor:
                for command in commands:
                    try:
                        cursor.execute(command)
                    except Exception as ex:
                        _logger.error("db-command failed: %s\n%s", ex, command)
                        raise

    @classmethod
    def query_one(cls, query: str):
        if not cls._postgresql:
            raise SetupTestException("Database not initialized!")

        with psycopg.connect(**SetupTest.get_database_params(psycopg_naming=True)) as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query)
                return cursor.fetchone()

    @classmethod
    def query_all(cls, query: str):
        if not cls._postgresql:
            raise SetupTestException("Database not initialized!")

        with psycopg.connect(**SetupTest.get_database_params(psycopg_naming=True)) as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query)
                return cursor.fetchall()
