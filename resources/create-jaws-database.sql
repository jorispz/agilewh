DROP DATABASE IF EXISTS `jawswh`;
DROP USER 'jawswh';
CREATE DATABASE `jawswh`;
CREATE USER 'jawswh' IDENTIFIED BY 'jawswh';
GRANT ALL ON `jawswh`.* TO 'jawswh';
