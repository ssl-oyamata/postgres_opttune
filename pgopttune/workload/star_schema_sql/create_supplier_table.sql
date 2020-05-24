CREATE TABLE IF NOT EXISTS supplier
(
    s_suppkey INTEGER PRIMARY KEY,
    s_name    CHAR(25),
    s_address TEXT,
    s_city    CHAR(10),
    s_nation  CHAR(15),
    s_region  CHAR(12),
    s_phone   CHAR(15),
    not_use     TEXT
);