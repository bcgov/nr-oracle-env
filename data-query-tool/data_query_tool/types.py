from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

LOGGER = logging.getLogger(__name__)


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
    SEQUENCE = 6


@dataclass
class Table:
    """
    Table / Schema combination.
    """

    table_name: str
    schema: str


@dataclass
class Dependency:
    """
    Dependency class
    """

    object_type: ObjectType
    object_name: str
    object_schema: str


# creating this alias, because in some cases it feels more appropriate
# to call something a Database obejct rather than a dependency
DbObject = Dependency


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
class DBDependencyMapping(Dependency):
    """
    Data class for table dependencies.

    This is a hierarchical/recursive data structure


    """

    dependency_list: list[DBDependencyMapping | None]

    def to_dict(self):
        LOGGER.debug("got here with %s", self)
        deps_list = []
        for dependency in self.dependency_list:
            deps_list.append(dependency.to_dict())
        db_dep_dict = {
            "object_type": self.object_type.name,
            "object_name": self.object_name,
            "object_schema": self.object_schema,
            "dependency_list": deps_list,
        }
        return db_dep_dict

    def as_dict(self):
        return self.to_dict()

    def to_str(self, indent: int = 0) -> str:
        out_str = f"{self.object_type.name}: {self.object_schema}.{self.object_name}"
        dep_str = "\n" + (" " * indent) + "--------- dependencies ---------\n"
        indent = indent + 4
        deps_list = []
        for dependency in self.dependency_list:
            deps_list.append((" " * indent) + dependency.to_str(indent=indent))
        if deps_list:
            out_str += dep_str
            out_str += "\n".join(deps_list)
        return out_str

    def __str__(self) -> str:
        """
        Render the data structure as a string.

        Creates a report representation of the data structure.

        :return: a text representation of the data structure
        :rtype: str
        """
        return self.to_str()
