DROP TABLE if exists users;
DROP TABLE if exists hosts;
DROP TABLE if exists services;
DROP TABLE if exists host_services;
DROP TABLE if exists dataset;

CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL
);


CREATE TABLE hosts (
id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
host_ip VARCHAR NOT NULL,
port INTEGER NOT NULL,
server_url VARCHAR NOT NULL,
container_description VARCHAR
);

CREATE TABLE services (
id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
service_type VARCHAR NOT NULL UNIQUE
);
CREATE TABLE host_services (
host_id               INTEGER NOT NULL,
service_id            INTEGER NOT NULL,
PRIMARY KEY (host_id, service_id),
FOREIGN KEY (host_id) REFERENCES hosts(id),
FOREIGN KEY (service_id) REFERENCES services(id)
);

CREATE TABLE dataset (
id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
dataset_name VARCHAR NOT NULL,
description VARCHAR,
url_path VARCHAR NOT NULL,
service_id INTEGER NOT NULL,
host_id INTEGER NOT NULL,
FOREIGN KEY (host_id) REFERENCES hosts(id),
FOREIGN KEY (service_id) REFERENCES services(id)
);
