# postgres_opttune
Trying PostgreSQL parameter tuning using machine learning.
(Currently beta version)

## Overview
postgres_opttune is a tool developed to tune PostgreSQL's parameters.
You can automatically find the appropriate settings PostgreSQL's parameters.

postgres_opttune optimizes parameters using one of the following workloads:
1. Pgbench
2. Oltpbenchmark
3. StarSchemaBenchmark

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
# yum install -y https://centos7.iuscommunity.org/ius-release.rpm
# yum install -y python36u python36u-devel python36u-libs python36u-pip
# pip3 install --upgrade pip setuptools
```

2. compiler(gcc) install
```
# yum install -y gcc
```

3. git install
```
# yum install -y git
```

4. PostgreSQL install
```
# yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
# yum install -y postgresql12 postgresql12-server postgresql12-libs postgresql12-contrib  postgresql12-devel
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

9. Set conditions for optimization with pgbench workload
```
# vi ~/postgres_opttune/conf/postgres_opttune.conf

    [turning]
    study_name = pgbench_study # study name
    benchmark = pgbench # benchmark tool
      :
    number_trail = 100 # number of benchmarks to run for turning
      :
    [pgbench]
    scale_factor = 10 # pgbench scale factor
    clients = 10 # number of clients
    evaluation_time = 300 # time of benchmark run
```

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
[XXXX-XX-XX XX:XX:XX,XXX] [INFO] Finished trial#0 resulted in value: 349.119024. Current best value is 349.119024 with parameters: {'bgwriter_lru_maxpages': 328, 'checkpoint_completion_target': 0.893929105997455, 'checkpoint_timeout': 287718, 'default_statistics_target': 622, 'effective_cache_size': 619997825, 'effective_io_concurrency': 308, 'max_parallel_maintenance_workers': 3, 'max_parallel_workers_per_gather': 4, 'max_wal_size': 12292573981, 'random_page_cost': 1.2751021753032283, 'shared_buffers': 216318893, 'temp_buffers': 630335709, 'wal_buffers': 162804587, 'wal_compression ': 'off', 'wal_writer_delay': 7416, 'work_mem': 368827108}.
[XXXX-XX-XX XX:XX:XX,XXX] [INFO] trail#1 conf saved : ./trial_conf/pgbench_study_#1_postgresql.conf
[XXXX-XX-XX XX:XX:XX,XXX] [INFO] Finished trial#1 resulted in value: 360.061211. Current best value is 360.061211 with parameters: {'bgwriter_lru_maxpages': 17, 'checkpoint_completion_target': 0.2971674609467846, 'checkpoint_timeout': 1212732, 'default_statistics_target': 1372, 'effective_cache_size': 972861787, 'effective_io_concurrency': 872, 'max_parallel_maintenance_workers': 2, 'max_parallel_workers_per_gather': 0, 'max_wal_size': 15027265966, 'random_page_cost': 8.950134901557504, 'shared_buffers': 388947193, 'temp_buffers': 734998561, 'wal_buffers': 48759960, 'wal_compression ': 'off', 'wal_writer_delay': 2003, 'work_mem': 463076169}.
[XXXX-XX-XX XX:XX:XX,XXX] [INFO] trail#2 conf saved : ./trial_conf/pgbench_study_#2_postgresql.conf
[XXXX-XX-XX XX:XX:XX,XXX] [INFO] Finished trial#2 resulted in value: 370.547247. Current best value is 370.547247 with parameters: {'bgwriter_lru_maxpages': 694, 'checkpoint_completion_target': 0.5476852995239162, 'checkpoint_timeout': 1389651, 'default_statistics_target': 1947, 'effective_cache_size': 698887583, 'effective_io_concurrency': 314, 'max_parallel_maintenance_workers': 2, 'max_parallel_workers_per_gather': 4, 'max_wal_size': 15091114481, 'random_page_cost': 9.659605392372143, 'shared_buffers': 361287815, 'temp_buffers': 686169910, 'wal_buffers': 255305498, 'wal_compression ': 'off', 'wal_writer_delay': 5803, 'work_mem': 1020437721}.
```

### Installation(Oltpbenchmark)
When using oltpbenchmark, perform the following procedure.

Item numbers 1 to 8 of the installation method are the same as pgbench.

9. Oltpbenchmark install
```
# yum install -y wget
# yum install -y java-1.8.0-openjdk-devel  
# wget http://ftp.yz.yamagata-u.ac.jp/pub/network/apache//ant/binaries/apache-ant-1.10.7-bin.tar.gz -P /usr/local/src 
# tar zxvf /usr/local/src/apache-ant-1.10.7-bin.tar.gz -C /opt  
# ln -s /usr/local/src/apache-ant-1.10.7 /opt/ant
# export PATH=$PATH:/opt/ant/bin
# cd /opt
# git clone https://github.com/oltpbenchmark/oltpbench  
# cd oltpbench
# ant bootstrap
# ant resolve 
# ant build
```

10. Set conditions for optimization with oltpbench workload
```
# vi ~/postgres_opttune/conf/postgres_opttune.conf

    [turning]
    study_name = oltpbench_study # study name
    benchmark = oltpbench # benchmark tool
      :
    number_trail = 100 # number of benchmarks to run for turning
      :
    [oltpbench]
    oltpbench_path = /opt/oltpbench # oltpbenchmark path
    benchmark_kind = tpcc # benchmark kind
    oltpbench_config_path = ./conf/tpcc_config_postgres.xml # config path
```

11. Edit oltpbench settings
```
# vi ~/postgres_opttune/conf/tpcc_config_postgres.xml

    ex.
    <!-- Scale factor is the number of warehouses in TPCC -->
    <scalefactor>50</scalefactor>  # Scale Factor in TPC-C

    <!-- The workload -->
    <terminals>100</terminals>  # 100 concurrent terminals
    <works>
        <work>
          <time>1800</time>  # observation time is 30 mins
```

12. Edit max_connections in postgresql.conf according to the number of concurrent connections for the oltpbench workload
```
# vi /var/lib/pgsql/12/data/postgresql.conf

    max_connections = 100   --->   150

# su - postgres
$ /usr/pgsql-12/bin/pg_ctl restart -D /var/lib/pgsql/12/data
$ exit
```

### Useage
1. Run Oltpbenchmark and parameter turning
```
# cd ~/postgres_opttune
# python3 tune.py
```

### Installation(StarSchemaBenchmark)
When use StarSchemaBenchmark, perform the following procedure.

Item numbers 1 to 8 of the installation method are the same as pgbench.

9. ssb-dbgen install
```
# cd /opt
# git clone https://github.com/greenlion/ssb-dbgen
# cd ssb-dbgen/
# make
```

10. Settings StarSchemaBenchmark workload
```
# vi ~/postgres_opttune/conf/postgres_opttune.conf

    [turning]
    study_name = ssb_study # study name
    benchmark = star_schema_benchmark # benchmark tool
      :
    number_trail = 100 # number of benchmarks to run for turning
      :
    [star-schema-benchmark]
    ssb_dbgen_path = /opt/ssb-dbgen # ssb-dbgen directory path
    scale_factor = 1 # ssb-dbgen sacle factor
    clients = 1 # Number of clients
    sql_file_path = ./pgopttune/workload/star_schema_sql/
    sql_key = Q1.1, Q2.1, Q3.1
```

### Useage
1. Running StarSchemaBenchmark and parameter turning
```
# cd ~/postgres_opttune
# python3 tune.py
```


## Test Platforms
|Category|Module Name|
|:--|:--:|
|OS|CentOS Linux release 7.6.1810|
|DBMS|PostgreSQL 12|
|Python|3.6|

## License
Apache License 2.0

