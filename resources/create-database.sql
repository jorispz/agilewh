DROP DATABASE IF EXISTS `agilewh`;
DROP USER 'agilewh';
CREATE DATABASE `agilewh`;
CREATE USER 'agilewh' IDENTIFIED BY 'agilewh';
GRANT ALL ON `agilewh`.* TO 'agilewh';
