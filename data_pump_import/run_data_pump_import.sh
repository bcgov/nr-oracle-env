# -- this is the one that works
impdp system/default@oracle:1521/DBDOCK_STRUCT_01 \
    REMAP_TABLESPACE=CONSEP_BIGFILE_TBS:USERS,THE_BIGFILE_TBS:USERS,USR01:USERS \
    dumpfile=dump_file.dmp

