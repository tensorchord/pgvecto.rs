/bin/python3 /home/envd/pgvecto.rs/bindings/python/examples/psycopg_testcpy.py &
PID=$(ps aux | grep "/usr/lib/postgresql/15/bin/postgres" | grep -v grep | awk '{print $2}')
# use perf to collect data
sudo /usr/lib/linux-tools/5.15.0-97-generic/perf record -e cpu-clock -F 1000 -g -p $PID -- sleep 30
sudo /usr/lib/linux-tools/5.15.0-97-generic/perf script -i perf.data > insert.out.perf