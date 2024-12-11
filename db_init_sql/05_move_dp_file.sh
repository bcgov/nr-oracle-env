# start by findout out what the datapump directory is
echo "SET sqlprompt ''"  > ./tmp.sql
echo "SET sqlnumber off"  >> ./tmp.sql
echo "SET verify off" >> ./tmp.sql
echo "SET pages 0" >> ./tmp.sql
echo "SET echo off" >> ./tmp.sql
echo "SET head off" >> ./tmp.sql
echo "SET feedback off" >> ./tmp.sql
echo "SET feed off" >> ./tmp.sql
echo "SET serveroutput OFF" >> ./tmp.sql
echo "SET escape '\'" >> ./tmp.sql
echo "select 'KEEP ' || TO_CHAR( directory_path ) from dba_directories where directory_name='DATA_PUMP_DIR';"  >> ./tmp.sql
echo "exit" >> ./tmp.sql
echo "endl" >> ./tmp.sql
oracle_datapump_directory=$(sqlplus system/default@oracle:1521/DBDOCK_STRUCT_01 @tmp.sql | grep KEEP | sed 's/KEEP//;s/[   ]//g') 
echo "oracle_datapump_directory=$oracle_datapump_directory"

echo "copying the dump file to the datapump directory...."
cp /dpdata/dump_file.dmp $oracle_datapump_directory
echo "done!"
