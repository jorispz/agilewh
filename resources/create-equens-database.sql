DROP DATABASE IF EXISTS `equenswh`;
DROP USER 'equenswh';
CREATE DATABASE `equenswh`;
CREATE USER 'equenswh' IDENTIFIED BY 'equenswhilewh';
GRANT ALL ON `equenswh`.* TO 'equenswh';
