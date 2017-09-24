-- source /FOLDER_PATH/nexmo-de-app/src/main/resources/setup.sql

select "Creating Database and User" as ctest_text;
CREATE DATABASE IF NOT EXISTS nexmode;
CREATE USER IF NOT EXISTS 'nexmo_user'@'%' IDENTIFIED BY 'nexmo';
GRANT ALL ON nexmode.* TO 'nexmo_user'@'%';

USE nexmode;

select "Creating log_data Table" as ctest_text;
drop table log_data;
CREATE TABLE IF NOT EXISTS `log_data` (
     id INT NOT NULL AUTO_INCREMENT,
     message_id VARCHAR(256) NOT NULL,
     timestamp TIMESTAMP(3) NOT NULL,
     account_id VARCHAR(256) NOT NULL,
     gateway_id VARCHAR(256) NOT NULL,
     country VARCHAR(256) NOT NULL,
     status VARCHAR(256) NOT NULL,
     price DECIMAL(12, 6) NOT NULL,
     cost DECIMAL(12, 6) NOT NULL,
     PRIMARY KEY(id)
 );

