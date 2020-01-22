CREATE TABLE interface_stable (
    id serial PRIMARY KEY,
    node_name varchar(32),
    data_name varchar(32),
    data_type varchar(32),
    version date,
    content json,
    update_time timestamp(6) DEFAULT now(),
    data_sign varchar(32),
    CONSTRAINT "stable_unique" UNIQUE ("version", "data_sign")
);

CREATE UNIQUE INDEX interferce_stable_index ON interface_stable(data_sign, version);