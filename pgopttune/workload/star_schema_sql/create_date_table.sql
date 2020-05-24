CREATE TABLE IF NOT EXISTS date
(
    d_datekey          DATE PRIMARY KEY,
    d_date             CHAR(18),
    d_dayofweek        CHAR(9),
    d_month            CHAR(9),
    d_year             INTEGER,
    d_yearmonthnum     INTEGER,
    d_yearmonth        CHAR(7),
    d_daynuminweek     INTEGER,
    d_daynuminmonth    INTEGER,
    d_daynuminyear     INTEGER,
    d_monthnuminyear   INTEGER,
    d_weeknuminyear    INTEGER,
    d_sellingseason    TEXT,
    d_lastdayinweekfl  CHAR(1),
    d_lastdayinmonthfl CHAR(1),
    d_holidayfl        CHAR(1),
    d_weekdayfl        CHAR(1),
    not_use              TEXT
);