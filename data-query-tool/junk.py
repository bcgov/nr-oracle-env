# from enum import Enum


# class ObjectType(Enum):
#     """
#     Define the database types that are supported.

#     An enumeration of the different database types that are supported by the
#     scripts in this project.

#     """

#     TABLE = 1
#     VIEW = 2
#     TRIGGER = 3
#     PROCEDURE = 4
#     FUNCTION = 5


# myEnum = ObjectType.TABLE

# print(myEnum.name)


# import packaging.version

# myVersion = packaging.version.parse("1.0.0")
# print(myVersion.release)

# print(myVersion.minor)


from sqlalchemy import MetaData, Table, create_engine

engine = create_engine(
    "oracle+oracledb://the:default@localhost:1522/?service_name=DBDOCK_STRUCT_01",
)
metadata = MetaData()


table_name = "seedlot"  # Replace with your table name
# table_name = "seedlot_parent_tree"
schema_name = "the"

# Reflect the database schema
# metadata.reflect(bind=engine, schema=schema_name)

# for table in metadata.sorted_tables:
#     print(f"Table: {table.name}")
#     for fk in table.foreign_keys:
#         print(f"  Depends on: {fk.column.table.name}")


def getRelatations(table_name):
    table = Table(table_name, metadata, schema=schema_name, autoload_with=engine)
    print(f"----------- {table_name} -----------")
    relations = []
    var = 0
    for fk in table.foreign_keys:
        print(
            f"  Table {table_name} depends on {fk.column.table.name} {fk.column.table.primary_key.name}"
        )
        var += 1
    print(f"record count: {var}")


getRelatations(table_name)


# table = Table(table_name, metadata, schema=schema_name, autoload_with=engine)

# for fk in table.foreign_keys:
#     print(
#         f"  Table {table_name} depends on {fk.column.table.name} {fk.column.table.primary_key.name}"
#     )
#     table2 = Table(
#         fk.column.table.name, metadata, schema=schema_name, autoload_with=engine
#     )
#     for fk2 in table2.foreign_keys:
#         print(
#             f"    Table {table_name} depends on {fk2.column.table.name} {fk2.column.table.primary_key.name}"
#         )

# print(fk)

# Print table dependencies
# for table in metadata.sorted_tables:
#     print(f"Table: {table.name}")
#     for fk in table.foreign_keys:
#         print(f"  Depends on: {fk.column.table.name}")
