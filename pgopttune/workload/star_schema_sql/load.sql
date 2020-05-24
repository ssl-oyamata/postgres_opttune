TRUNCATE customer, date, part, supplier, lineorder;
COPY customer
    FROM
    '/tmp/customer.tbl' DELIMITER '|';
COPY date
    FROM
    '/tmp/date.tbl' DELIMITER '|';
COPY part
    FROM
    '/tmp/part.tbl' DELIMITER '|';
COPY supplier
    FROM
    '/tmp/supplier.tbl' DELIMITER '|';
COPY lineorder
    FROM
    '/tmp/lineorder.tbl' DELIMITER '|';