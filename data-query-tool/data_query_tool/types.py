from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ObjectType(Enum):
    """
    Define the database types that are supported.

    An enumeration of the different database types that are supported by the
    scripts in this project.

    """

    TABLE = 1
    VIEW = 2
    TRIGGER = 3
    PROCEDURE = 4
    FUNCTION = 5


@dataclass
class OCParams:
    """
    Data class for OC connection parameters.

    * host: the host for the OC service
    * token: the token used to validate api requests

    """

    host: str | None
    token: str | None
    namespace: str | None


@dataclass
class ConnectionParameters:
    """
    Data class for database connection parameters.

    * username: the username to connect to the database with
    * password: the password for the username
    * host: the host for the database
    * port: the port for the database
    * service_name: the service name for the database

    """

    username: str | None
    password: str | None
    host: str | None
    port: str | None
    service_name: str | None


@dataclass
class ObjectStoreParameters:
    """
    Parameters used to connect to object store buckets.

    """

    user_id: str
    bucket: str
    host: str
    secret: str


@dataclass
class AppConstants:
    """
    app constants.

    constants used to connect to backing services (database / object store /
    etc)

    """

    db_conn_params: ConnectionParameters
    object_store_params: ObjectStoreParameters
    schema_to_sync: str


@dataclass
class DBDependencyMapping:
    """
    Data class for table dependencies.

    This is a hierarchical/recursive data structure


    """

    object_type: ObjectType
    object_name: str
    object_schema: str
    dependency_list: list[DBDependencyMapping | None]
