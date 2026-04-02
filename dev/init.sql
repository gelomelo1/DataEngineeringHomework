CREATE USER airflowuser WITH PASSWORD 'airflowpwd';
CREATE DATABASE airflowdb;
GRANT ALL PRIVILEGES ON DATABASE airflowdb TO airflowuser;
\c airflowdb
GRANT ALL PRIVILEGES ON SCHEMA public TO airflowuser;