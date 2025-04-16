"""
Iteratively calling the dependencies for all the objects that relate to the
silva stuff.
"""

import json
import logging
import logging.config
import pathlib
import sys

import data_query_tool.constants as constants
import data_query_tool.oralib2 as oralib2
import data_query_tool.types as types

LOGGER = None


class multi_deps:
    def __init__(
        self,
        table_list_file: pathlib.Path,
        schema: str,
        deps_dir: pathlib.Path,
    ):
        self.logging_config()
        LOGGER.debug("table_list = %s", table_list_file)
        self.table_list_file = table_list_file
        self.deps_dir = deps_dir
        self.schema = schema
        self.ora = None
        self.db_conn()
        self.processed = oralib2.ProcessedObjects()

    def logging_config(self) -> None:
        """
        Configure the logging for the cli.
        """
        log_conf_path = pathlib.Path(__file__).parent / "logging.config"
        logging.config.fileConfig(log_conf_path)
        logger = logging.getLogger(__name__)
        logger.debug("testing logger config...")
        global LOGGER  # noqa: PLW0603
        LOGGER = logger

    def db_conn(self):
        db_cons = constants.get_database_connection_parameters()
        self.ora = oralib2.Oracle(db_cons)

    def run_all(self):
        """
        Generate migrtations for all the tables in the supplied list.
        """
        all_deps = []
        deps = None
        # iterates over each new table creating a new migration
        with self.table_list_file.open("r") as f:
            for record in f:
                table_name = record.split(",")[0]
                table_name = table_name.strip()
                json_file_path = self.get_json_file_path(table_name)
                text_file_path = self.get_text_file_path(table_name)
                if not json_file_path.exists():
                    LOGGER.debug("get deps for: %s", table_name)
                    object_type = self.ora.get_object_type(
                        object_name=table_name,
                        schema=self.schema,
                    )
                    deps = self.ora.get_db_object_deps(
                        object_name=table_name,
                        schema=schema.upper(),
                        object_type=object_type,
                    )

                    deps_dict = deps.to_dict()
                    with json_file_path.open("w") as json_writer:
                        json.dump(deps_dict, json_writer, indent=4)
                else:
                    deps = self.ora.load_json_deps(json_file=json_file_path)
                all_deps.append(deps)
                if not text_file_path.exists():
                    with text_file_path.open("w") as text_writer:
                        text_to_write = deps.to_str()
                        text_writer.write(text_to_write)
        return all_deps

    def get_json_file_path(self, object_name):
        file_name = f"{object_name}.json"
        full_path = self.deps_dir / "json" / file_name
        return full_path

    def get_text_file_path(self, object_name):
        file_name = f"{object_name}.txt"
        full_path = self.deps_dir / "txt" / file_name
        return full_path

    def flatten(self):
        all_deps = self.run_all()
        for cur_dep in all_deps:
            self.flat_dep(cur_dep)

        for key in self.processed.processed_objects.keys():
            self.processed.processed_objects[key].sort()

        jsonstr = json.dumps(self.processed.processed_objects, indent=4)
        LOGGER.debug("jsonstr: \n%s", jsonstr)

    def flat_dep(self, dep: types.DBDependencyMapping):
        self.processed.add(dep.object_name, dep.object_type)
        if dep.dependency_list:
            for child_dep in dep.dependency_list:
                self.flat_dep(child_dep)


if __name__ == "__main__":
    batch_file = pathlib.Path("tablelist.txt")
    schema = "THE"
    deps_dir = pathlib.Path(
        "/home/kjnether/fsa_proj/nr-fsa-orastruct/project_specific/silva/deps"
    )
    deps = multi_deps(
        table_list_file=batch_file,
        schema=schema,
        deps_dir=deps_dir,
    )
    LOGGER.debug("deps_dir: %s", deps_dir)

    deps.run_all()
    # deps.flatten()
