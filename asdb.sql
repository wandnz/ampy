CREATE TABLE asmap (
   asn INTEGER NOT NULL  PRIMARY KEY,
   asname TEXT NOT NULL
);

GRANT ALL ON ALL TABLES IN SCHEMA public to "ampweb";
