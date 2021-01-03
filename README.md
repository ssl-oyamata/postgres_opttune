## postgres_opttune
postgres_opttune is a tool developed to tune PostgreSQL's parameters.
You can automatically find the appropriate settings PostgreSQL's parameters.

The following graph shows the result of optimizing PostgreSQL 12 using the workload of Oltpbenchmark.
![postgres_opttune](https://user-images.githubusercontent.com/22017773/73520567-42a99c00-4447-11ea-84cb-9620ff58f228.png)

## Created by
[to-aoki](https://github.com/to-aoki), [git-itake](https://github.com/git-itake), [ssl-oyamata](https://github.com/ssl-oyamata)

## Example
The following procedure is for installing PostgreSQL12 and postgres_opttune on a single server.

### Installation(pgbench)
When tuning using pgbench, perform the following procedure.

1. python3 install
```
# yum install python3 python3-devel python3-libs python3-pip
# pip3 install --upgrade pip setuptools
```

2. compiler(gcc) install
```
# yum install gcc
```

3. git install
```
# yum install git
```

4. PostgreSQL install
```
# yum install epel-release centos-release-scl
# yum install https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
# yum install postgresql12 postgresql12-server postgresql12-libs postgresql12-contrib  postgresql12-devel
```

5. Create Database 
```
# su - postgres
$ /usr/pgsql-12/bin/initdb -E UTF8 --no-locale /var/lib/pgsql/12/data
$ /usr/pgsql-12/bin/pg_ctl start -D /var/lib/pgsql/12/data
$ /usr/pgsql-12/bin/createdb tpcc
$ exit
```

6. Get `postgres_opttune`
```
# cd ~
# git clone https://github.com/ssl-oyamata/postgres_opttune
```

7. postgres_opttune requirements install (Requires path to pg_config)
```
# export PATH=$PATH:/usr/pgsql-12/bin
# pip3 install -r ~/postgres_opttune/requirements.txt
```

8. Allow sudo from postgres without password
```
# visudo

    # %wheel        ALL=(ALL)       NOPASSWD: ALL
    ↓
    # %wheel        ALL=(ALL)       NOPASSWD: ALL
    postgres        ALL=(ALL)       NOPASSWD: ALL
```

9. set postgres user password
```
# passwd postgres 
```

10. Set conditions for optimization with pgbench workload
```
# vi ~/postgres_opttune/conf/postgres_opttune.conf

    [turning]
    benchmark = my_workload # Benchmark tool name('my_workload' or 'sampled_workload' or 'pgbench' or 'oltpbench' or 'star_schema_benchmark')
      :
    number_trail = 100 # Number of benchmarks to run for turning
    data_load_interval = 1 # Specify the data load interval by the number of benchmarks
      :
    [my-workload]
    data_load_command = /usr/pgsql-12/bin/pgbench -i -s 10 tpcc
    run_workload_command = /usr/pgsql-12/bin/pgbench tpcc -T 360
```

The data_load_command parameter specifies the command to execute the data load, 
and the run_workload_command parameter specifies the command to execute the workload to be optimized.

### Useage
1. Run pgbench and parameter turning
```
# cd ~/postgres_opttune
# python3 tune.py
```
#### Output Sample
```
Run benchmark : pgbench
[XXXX-XX-XX XX:XX:XX,XXX] [INFO] trail#0 conf saved : ./trial_conf/pgbench_study_#0_postgresql.conf
[XXXX-XX-XX XX:XX:XX,XXX] [INFO] Finished trial#0 with value: 481.397632 with parameters: {'bgwriter_lru_maxpages': 357, 'checkpoint_completion_target': 0.40910329092334863, 'checkpoint_timeout': 86400000, 'default_statistics_target': 1542, 'effective_cache_size': 527301023, 'effective_io_concurrency': 996, 'max_parallel_maintenance_workers': 1, 'max_parallel_workers': 0, 'max_parallel_workers_per_gather': 0, 'max_wal_size': 8226078720, 'max_worker_processes': 1, 'random_page_cost': 5.750358533002629, 'shared_buffers': 408398531, 'temp_buffers': 563302226, 'wal_buffers': 47817153, 'wal_compression ': 'on', 'wal_writer_delay': 9871, 'work_mem': 38677995}. Best is trial#0 with value: 481.397632.
[XXXX-XX-XX XX:XX:XX,XXX] [INFO] trail#1 conf saved : ./trial_conf/pgbench_study_#1_postgresql.conf
[XXXX-XX-XX XX:XX:XX,XXX] [INFO] Finished trial#1 with value: 452.135988 with parameters: {'bgwriter_lru_maxpages': 652, 'checkpoint_completion_target': 0.2466899683264132, 'checkpoint_timeout': 86400000, 'default_statistics_target': 579, 'effective_cache_size': 453660418, 'effective_io_concurrency': 728, 'max_parallel_maintenance_workers': 1, 'max_parallel_workers': 0, 'max_parallel_workers_per_gather': 1, 'max_wal_size': 8226078720, 'max_worker_processes': 1, 'random_page_cost': 8.029232529773868, 'shared_buffers': 744365966, 'temp_buffers': 63186917, 'wal_buffers': 227117996, 'wal_compression ': 'on', 'wal_writer_delay': 7946, 'work_mem': 401474526}. Best is trial#0 with value: 481.397632.
:
[XXXX-XX-XX XX:XX:XX,XXX] [INFO] best trial : #41
best param : {'bgwriter_lru_maxpages': 355, 'checkpoint_completion_target': 0.5501787081736287, 'checkpoint_timeout': 86400000, 'default_statistics_target': 383, 'effective_cache_size': 466332707, 'effective_io_concurrency': 260, 'max_parallel_maintenance_workers': 1, 'max_parallel_workers': 1, 'max_parallel_workers_per_gather': 0, 'max_wal_size': 8226078720, 'max_worker_processes': 0, 'random_page_cost': 4.4395657949000356, 'shared_buffers': 752153294, 'temp_buffers': 201811454, 'wal_buffers': 2299422, 'wal_compression ': 'off', 'wal_writer_delay': 6238, 'work_mem': 258511024}
```

2. Check the results using the dashboard
```
# optuna-dashboard sqlite:///study-history.db --host xxx.xxx.xxx.xxx
```
![dashboard](https://user-images.githubusercontent.com/4098820/103479745-15156180-4e13-11eb-9c69-4a0cfd6e54f2.png)

## Test Platforms
|Category|Module Name|
|:--|:--:|
|OS|CentOS Linux release 7.8.2003|
|DBMS|PostgreSQL 12|
|Python|3.6|

## License
Apache License 2.0

