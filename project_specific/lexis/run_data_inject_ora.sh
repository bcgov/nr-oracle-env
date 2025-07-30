# chown -R derek:derek /app/data
# exec gosu derek "$@"

env
if [ "$(id -u)" = "0" ]; then
  chown -R derek:derek /app/data
  exec gosu derek python db_env_utils/main_ingest.py ORA PROD
else
  exec python db_env_utils/main_ingest.py ORA PROD
fi
