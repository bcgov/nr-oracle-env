import pathlib

import duckdb

files_2_delete = []


def count_records_in_duckdb(db_path):
    #print(f"\nDatabase: {db_path}")
    con = duckdb.connect(str(db_path))
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    for table in tables:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        # print(f"  Table: {table} - Records: {count}")

        # if count < 2000:
        #     files_2_delete.append(db_path)
        if count > 1000000:
            print(f"  Table: {table} - Records: {count}")
    con.close()

def delete_smalldb_files():
    for file in files_2_delete:
        print(f"Deleting small database file: {file}")
        file.unlink()
    files_2_delete.clear()

# Change this to the directory containing your DuckDB files
duckdb_dir = pathlib.Path("/home/kjnether/fsa_proj/nr-fsa-orastruct/project_specific/lexis/data/PROD/ORA")
for db_file in duckdb_dir.glob("*.ddb"):
    count_records_in_duckdb(db_file)
    delete_smalldb_files()