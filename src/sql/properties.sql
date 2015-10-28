# Host: localhost  (Version: 5.5.32)
# Date: 2015-10-27 10:45:25
# Generator: MySQL-Front 5.3  (Build 4.234)

/*!40101 SET NAMES latin1 */;

#
# Structure for table "properties"
#

CREATE TABLE `properties` (
  `property_name` varchar(255) DEFAULT NULL,
  `property_value` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

#
# Data for table "properties"
#

INSERT INTO `properties` VALUES ('tableau.username','admin'),('tableau.password','isat'),('tableau.siteUrl',NULL),('tableau.server.host','http://localhost'),('tableau.schema.location','res/ts-api-2_0.xsd'),('max_date.sales_prepaid','2015-10-27 10:32:00'),('hadoop.driver','org.apache.hive.jdbc.HiveDriver'),('hadoop.connection','jdbc:hive2://192.168.0.101:22/default'),('hadoop.username','mapr'),('hadoop.password','mapr2015'),('tableau.datasource.sales_prepaid.file','tde/SalesPrepaid.tde'),('tableau.datasource.sales_prepaid.name','Sales Prepaid');
