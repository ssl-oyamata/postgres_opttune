CREATE TABLE IF NOT EXISTS part
(
    p_partkey   INTEGER PRIMARY KEY,
    p_name      TEXT,
    p_mfgr      CHAR(6),
    p_category  CHAR(7),
    p_brand1    CHAR(9),
    p_color     CHAR(11),
    p_type      TEXT,
    p_size      INTEGER,
    p_container CHAR(10),
    not_use       TEXT
);