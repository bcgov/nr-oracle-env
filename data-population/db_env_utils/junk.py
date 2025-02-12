import os

import env_config
import geopandas
import geopandas as gpd
import oradb_lib
import pandas
from shapely.wkt import loads


def load_spatial():
    db_params = env_config.ConnectionParameters(
        username=os.getenv("ORACLE_USER_LOCAL"),
        password=os.getenv("ORACLE_PASSWORD_LOCAL"),
        host=os.getenv("ORACLE_HOST_LOCAL"),
        port=os.getenv("ORACLE_PORT_LOCAL"),
        service_name=os.getenv("ORACLE_SERVICE_LOCAL"),
        schema_to_sync=os.getenv("ORACLE_SCHEMA_TO_SYNC_LOCAL"),
    )
    db = oradb_lib.OracleDatabase(db_params)


def get_spatial():

    db_params = env_config.ConnectionParameters(
        username=os.getenv("ORACLE_USER_TEST"),
        password=os.getenv("ORACLE_PASSWORD_TEST"),
        host=os.getenv("ORACLE_HOST_TEST"),
        port=os.getenv("ORACLE_PORT_TEST"),
        service_name=os.getenv("ORACLE_SERVICE_TEST"),
        schema_to_sync=os.getenv("ORACLE_SCHEMA_TO_SYNC_LOCAL"),
    )

    db = oradb_lib.OracleDatabase(db_params)

    # Oracle connection details
    db.get_connection()
    # Query to fetch data and convert SDO_GEOMETRY to WKT
    query = """
      SELECT
        SUBMISSION_ID,INVASIVE_PLANT_ID,ORG_UNIT_NO,IAPP_SUBMISSION_STATUS_CODE,GENUS_CODE,SPECIES_CODE,SURVEY_DATE,
        SDO_UTIL.TO_WKTGEOMETRY(geometry) AS GEOMETRY,
        UTM_EASTING,UTM_NORTHING,UTM_ZONE,LOCATION_DESCRIPTION,ESTIMATED_AREA,SUBMITTER_LAST_NAME,SUBMITTER_FIRST_NAME,SUBMITTER_MIDDLE_INITIAL,SUBMITTER_PHONE_NUMBER,SUBMITTER_EMAIL_ADDRESS,SUBMITTER_COMMENTS,REVIEWER_COMMENTS,ENTRY_TIMESTAMP,ENTRY_USERID,UPDATE_TIMESTAMP,UPDATE_USERID,REVISION_COUNT
      FROM
          THE.INVASIVE_PLANT_SUBMISSION
    """

    # Fetch data into a pandas DataFrame
    df = pandas.read_sql(query, db.connection)
    print(df.columns)

    df["GEOMETRY"] = df["GEOMETRY"].astype(str)
    df["GEOMETRY"] = df["GEOMETRY"].apply(
        lambda wkt: loads(wkt) if wkt else None
    )

    gdf = gpd.GeoDataFrame(df, geometry="GEOMETRY", crs="EPSG:4326")

    # Save to Parquet
    gdf.to_parquet("oracle_data_spatial.parquet")

    # df.to_parquet(
    #     "spatial_data.parquet",
    #     engine="pyarrow",
    # )


def copyspatial_gpand():
    # conn_str = "OCI:user/password@host:port/sid"

    conn_str = f"OCI:{os.getenv('ORACLE_USER_TEST')}/{os.getenv('ORACLE_PASSWORD_TEST')}@{os.getenv('ORACLE_HOST_TEST')}:{os.getenv('ORACLE_PORT_TEST')}/{os.getenv('ORACLE_SERVICE_TEST')}"
    gdf = gpd.read_file(conn_str, layer="THE.INVASIVE_PLANT_SUBMISSION")
    gdf.to_parquet("output_geodata.parquet")

    # gdf = gpd.read_file("spatial_data.parquet")
    # gdf.to_file("spatial_data.geojson", driver="GeoJSON")


if __name__ == "__main__":
    get_spatial()
    # copyspatial_gpand()
