DROP DATABASE IF EXISTS `equenswh`;
DROP USER 'equenswh';
CREATE DATABASE `equenswh`;
CREATE USER 'equenswh' IDENTIFIED BY 'equenswh';
GRANT ALL ON `equenswh`.* TO 'equenswh';
