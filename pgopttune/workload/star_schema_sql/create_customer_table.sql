CREATE TABLE IF NOT EXISTS customer
(
    c_custkey    INTEGER PRIMARY KEY,
    c_name       TEXT,
    c_address    TEXT,
    c_city       CHAR(10),
    c_nation     CHAR(15),
    c_region     CHAR(12),
    c_phone      CHAR(15),
    c_mktsegment CHAR(10),
    not_use        TEXT
);