CREATE DATABASE  IF NOT EXISTS `coco` /*!40100 DEFAULT CHARACTER SET utf8mb3 */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `coco`;
-- MySQL dump 10.13  Distrib 8.0.32, for Win64 (x86_64)
--
-- Host: localhost    Database: coco
-- ------------------------------------------------------
-- Server version	8.0.32

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `account`
--

DROP TABLE IF EXISTS `account`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `account` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `createdby` varchar(255) DEFAULT NULL,
  `createddate` datetime(6) DEFAULT NULL,
  `modifiedby` varchar(255) DEFAULT NULL,
  `modifieddate` datetime(6) DEFAULT NULL,
  `city` varchar(255) DEFAULT NULL,
  `district` varchar(255) DEFAULT NULL,
  `email` varchar(255) DEFAULT NULL,
  `fullname` varchar(255) DEFAULT NULL,
  `grade` double DEFAULT NULL,
  `password` varchar(255) DEFAULT NULL,
  `phone` varchar(255) DEFAULT NULL,
  `point_address` varchar(255) DEFAULT NULL,
  `username` varchar(255) DEFAULT NULL,
  `ward` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `account`
--

LOCK TABLES `account` WRITE;
/*!40000 ALTER TABLE `account` DISABLE KEYS */;
INSERT INTO `account` VALUES (1,NULL,'2024-04-16 22:52:06.063000',NULL,'2024-06-01 16:36:56.062000',NULL,NULL,'buianhdo2002@gamil.com','bui anh do',5492.68,'$2a$10$lTZ4hu0dRIXq7lyaMKt/nuDixcx/yQKYSkptsUSHEEOhdLbMhjbpu','0123159',NULL,'anhdobui',NULL),(2,NULL,'2024-04-16 23:04:23.580000',NULL,'2024-04-16 23:04:23.661000',NULL,NULL,'buido2002@gamil.com','bui anh do',0,'$2a$10$nkSPDAQ3LpbsDsMDGjWhWugjCNAc3LxYXpRwd1GCSJoIl.3iosL0O','01231589',NULL,'anhdobui2002',NULL),(6,NULL,'2024-05-22 16:54:02.300000',NULL,'2024-05-22 18:10:34.521000',NULL,NULL,'nguyenvana@gmail.com','Nguyễn Văn A',2250.85,'$2a$10$/lvPxnvyFW6IH0R7XeFnbek.8bhDgzB.C3y8LJXQmDGGcqAB3WloC','096941256',NULL,'nguyenvana',NULL),(7,NULL,'2024-05-22 17:43:27.314000',NULL,'2024-05-22 17:48:02.883000',NULL,NULL,'dattt@gmail.com','Trần Tiến Đạt',1337.26,'$2a$10$DU6AuOT1eOzgYPQJdxlkwuDkFO1bojx9cUaJr3hh0mAR2TNbfcBJO','123456789',NULL,'tiendat',NULL),(8,NULL,'2024-05-27 22:00:36.121000',NULL,'2024-05-27 22:00:36.230000',NULL,NULL,'ttt0611@gamil.com','ng qu t',0,'$2a$10$TTS162ykCul.10UQc7m95ue07dGqTKJa6wbcwePwXsLHsCe/oGFta','012345678926',NULL,'ttt9',NULL),(9,NULL,'2024-05-27 22:02:54.913000',NULL,'2024-05-27 22:02:54.919000',NULL,NULL,'ttt0611@gamil.com','nguyen thi a',0,'$2a$10$G9/.LcpVB8nzrvFopt9qxeDaNZh9SLccOVevX4J44favk1S5vE5jy','012345678926',NULL,'nguyenthia',NULL),(10,NULL,'2024-05-27 22:05:14.741000',NULL,'2024-05-27 22:05:14.755000',NULL,NULL,'ttt0611@gamil.com','nguyen thi b',0,'$2a$10$OZ8IAlb8IlVILCT0h6Xf2O5H0NFQ0zm/cgWYXNIsy5emiCydoU6WG','012345678926',NULL,'nguyenthib',NULL),(11,NULL,'2024-05-27 22:08:45.673000',NULL,'2024-05-27 22:08:45.688000',NULL,NULL,'ttt0611@gamil.com','nguyen thi c',0,'$2a$10$zojFHIzd/7ACtvh0nIVhleZn57Yw3p2K6JIkvBB6MJB7L/QcFHZcW','012345678926',NULL,'nguyenthic',NULL),(12,NULL,'2024-05-27 22:15:03.477000',NULL,'2024-05-27 22:15:03.480000',NULL,NULL,'ttt0611@gamil.com','nguyen thi d',0,'$2a$10$TIC9cdXJWUoSGzLYjB4gc.eKrLdNVM007/MgxIVT47HJ6wEV2wolO','012345678926',NULL,'nguyenthid',NULL),(13,NULL,'2024-05-28 08:06:09.007000',NULL,'2024-05-28 08:06:09.020000',NULL,NULL,'ttt0611@gamil.com','nguyen thi e',0,'$2a$10$4dqUuus8zKfWUyfBe0ekZex4dxkR6kjsd.FVs4C3Fpwm6rZ35V.v6','012345678926',NULL,'nguyenthie',NULL),(14,NULL,'2024-05-28 08:08:10.282000',NULL,'2024-05-28 08:08:10.284000',NULL,NULL,'ttt0611@gamil.com','nguyen thi e',0,'$2a$10$SeS.zehICtxPqln.UuKfuutm1MpPVD56kh1Bo0G7RURPHVMcKphe.','012345678926',NULL,'nguyenthif',NULL),(15,NULL,'2024-06-01 15:48:40.299000',NULL,'2024-06-01 16:47:38.516000',NULL,NULL,'','',21.9,'$2a$10$KKvIhkXtZFzoBulTX3VEiuACceFS3Eft57tXf7EwWrJ4H9wrgNqQ.','',NULL,'admin',NULL);
/*!40000 ALTER TABLE `account` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `album`
--

DROP TABLE IF EXISTS `album`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `album` (
  `painting_id` bigint NOT NULL,
  `url` varchar(255) DEFAULT NULL,
  KEY `FK36kb1f8qtefa5jvki47ixtkkb` (`painting_id`),
  CONSTRAINT `FK36kb1f8qtefa5jvki47ixtkkb` FOREIGN KEY (`painting_id`) REFERENCES `painting` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `album`
--

LOCK TABLES `album` WRITE;
/*!40000 ALTER TABLE `album` DISABLE KEYS */;
INSERT INTO `album` VALUES (41,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716106842/10403509-ZFMIZJNU-6.jpg'),(41,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716106844/additional_53c0d608fc6b2b3e42f6bef1a2f9537000e90d99-AICC2-7.jpg'),(41,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716106845/additional_f3a0bdded06f70ca2828994fea3029c58eccadc7-AICC2-7.jpg'),(42,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107116/2820411-HSC00001-6.jpg'),(42,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107118/additional_1c776dc32d3288eeadc2b80bc59ade0337516db0-AICC2-7.jpg'),(42,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107120/additional_2a299da747d795a7f1f3975d4f2406b11a368673-AICC2-7.jpg'),(42,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107121/additional_c6b8f99a83c5ab5712a51abd41b5fd8dc33355df-AICC2-7.jpg'),(44,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107397/7321484-LGKDDESD-6.jpg'),(44,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107398/additional_52df53313407dcc4a1e449186cf0b4c737837f3f-AICC2-7.jpg'),(44,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107400/additional_d693a943cf509c8d73199ce3363fab98d7d50caf-AICC2-7.jpg'),(45,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107561/10419515-GZGAKLNT-7.jpg'),(46,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107691/10580977-YRBSBJKZ-7.jpg'),(47,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107777/10220045-AYIXZUEN-7.jpg'),(47,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107780/additional_6ef81951726d8834906a933f7bc7140db56f80fb-AICC2-7.jpg'),(48,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107930/2060217-JIWWEIBB-7.jpg'),(48,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107931/additional_c34f799c79e653ee08f0e812658975d2af9c1529-AICC2-7.jpg'),(50,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1712826532/7532880-HSC00001-6.jpg'),(50,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1712747040/8720961-MIODUCGC-6.jpg'),(51,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1715910500/image-7.jpg'),(51,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1712762976/Mona_Lisa.jpg'),(43,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1715910157/image-5.jpg');
/*!40000 ALTER TABLE `album` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cart`
--

DROP TABLE IF EXISTS `cart`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `cart` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `createdby` varchar(255) DEFAULT NULL,
  `createddate` datetime(6) DEFAULT NULL,
  `modifiedby` varchar(255) DEFAULT NULL,
  `modifieddate` datetime(6) DEFAULT NULL,
  `status` int DEFAULT NULL,
  `acc_id` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FKexcdd1pex25twxkpsltwk2y7w` (`acc_id`),
  CONSTRAINT `FKexcdd1pex25twxkpsltwk2y7w` FOREIGN KEY (`acc_id`) REFERENCES `account` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=59 DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cart`
--

LOCK TABLES `cart` WRITE;
/*!40000 ALTER TABLE `cart` DISABLE KEYS */;
INSERT INTO `cart` VALUES (8,NULL,'2024-04-18 00:42:18.939000',NULL,'2024-05-21 21:11:20.573000',0,1),(9,NULL,'2024-04-20 09:19:50.984000',NULL,'2024-04-20 09:19:51.096000',0,2),(10,NULL,'2024-04-20 14:09:47.211000',NULL,'2024-04-20 16:35:08.890000',0,2),(13,NULL,'2024-05-15 10:35:04.005000',NULL,'2024-05-15 10:35:04.086000',1,2),(14,NULL,'2024-05-21 21:12:06.838000',NULL,'2024-05-22 09:28:12.941000',0,1),(15,NULL,'2024-05-22 09:28:37.412000',NULL,'2024-05-22 09:29:38.852000',0,1),(16,NULL,'2024-05-22 09:31:51.334000',NULL,'2024-05-22 09:32:18.806000',0,1),(17,NULL,'2024-05-22 09:51:31.122000',NULL,'2024-05-22 12:51:45.612000',0,1),(18,NULL,'2024-05-22 12:51:45.658000',NULL,'2024-05-22 13:54:18.345000',0,1),(19,NULL,'2024-05-22 13:54:18.398000',NULL,'2024-05-22 14:01:00.721000',0,1),(20,NULL,'2024-05-22 14:01:00.766000',NULL,'2024-05-22 16:48:52.269000',0,1),(21,NULL,'2024-05-22 16:48:52.308000',NULL,'2024-05-22 22:05:42.911000',0,1),(22,NULL,'2024-05-22 16:54:26.271000',NULL,'2024-05-22 16:54:56.281000',0,6),(23,NULL,'2024-05-22 16:54:56.321000',NULL,'2024-05-22 16:59:34.103000',0,6),(24,NULL,'2024-05-22 16:59:34.140000',NULL,'2024-05-22 16:59:57.455000',0,6),(25,NULL,'2024-05-22 16:59:57.495000',NULL,'2024-05-22 17:39:26.541000',0,6),(26,NULL,'2024-05-22 17:39:26.633000',NULL,'2024-05-22 17:39:26.637000',1,6),(27,NULL,'2024-05-22 17:44:00.261000',NULL,'2024-05-22 17:45:09.839000',0,7),(28,NULL,'2024-05-22 17:45:09.893000',NULL,'2024-05-22 17:46:12.314000',0,7),(29,NULL,'2024-05-22 17:46:12.368000',NULL,'2024-05-22 17:47:34.735000',0,7),(30,NULL,'2024-05-22 17:47:34.794000',NULL,'2024-05-23 08:01:37.501000',0,7),(31,NULL,'2024-05-22 22:05:42.980000',NULL,'2024-05-28 07:28:28.869000',0,1),(32,NULL,'2024-05-23 08:01:37.539000',NULL,'2024-05-23 08:01:37.539000',1,7),(33,NULL,'2024-05-28 07:28:28.917000',NULL,'2024-05-28 07:35:17.593000',0,1),(34,NULL,'2024-05-28 07:35:17.619000',NULL,'2024-05-28 07:36:57.410000',0,1),(35,NULL,'2024-05-28 07:36:57.444000',NULL,'2024-05-28 07:49:07.068000',0,1),(36,NULL,'2024-05-28 07:49:07.087000',NULL,'2024-05-28 08:05:16.187000',0,1),(37,NULL,'2024-05-28 08:05:16.206000',NULL,'2024-05-28 16:14:32.990000',0,1),(38,NULL,'2024-05-28 16:14:33.029000',NULL,'2024-05-28 17:39:48.428000',0,1),(39,NULL,'2024-05-28 17:39:48.454000',NULL,'2024-05-28 17:41:53.231000',0,1),(40,NULL,'2024-05-28 17:41:53.252000',NULL,'2024-05-28 17:43:37.466000',0,1),(41,NULL,'2024-05-28 17:43:37.494000',NULL,'2024-05-28 17:44:56.586000',0,1),(42,NULL,'2024-05-28 17:44:56.619000',NULL,'2024-05-28 17:48:53.903000',0,1),(43,NULL,'2024-05-28 17:48:53.934000',NULL,'2024-05-28 17:49:22.893000',0,1),(44,NULL,'2024-05-28 17:49:22.915000',NULL,'2024-05-28 17:51:54.606000',0,1),(45,NULL,'2024-05-28 17:51:54.629000',NULL,'2024-05-28 17:56:14.317000',0,1),(46,NULL,'2024-05-28 17:56:14.348000',NULL,'2024-05-28 18:37:43.036000',0,1),(47,NULL,'2024-05-28 18:37:43.071000',NULL,'2024-05-29 06:56:57.402000',0,1),(48,NULL,'2024-05-29 06:56:57.444000',NULL,'2024-05-29 06:57:30.905000',0,1),(49,NULL,'2024-05-29 06:57:30.931000',NULL,'2024-05-29 07:12:35.908000',0,1),(50,NULL,'2024-05-29 07:12:35.941000',NULL,'2024-05-29 08:44:39.951000',0,1),(51,NULL,'2024-05-29 08:44:39.988000',NULL,'2024-05-29 08:45:32.826000',0,1),(52,NULL,'2024-05-29 08:45:32.858000',NULL,'2024-05-29 08:51:40.423000',0,1),(53,NULL,'2024-05-29 08:51:40.456000',NULL,'2024-05-29 08:52:20.363000',0,1),(54,NULL,'2024-05-29 08:52:20.382000',NULL,'2024-05-29 10:13:00.193000',0,1),(55,NULL,'2024-05-29 10:13:00.256000',NULL,'2024-05-29 10:13:30.197000',0,1),(56,NULL,'2024-05-29 10:13:30.238000',NULL,'2024-05-29 10:13:30.238000',1,1),(57,NULL,'2024-06-01 15:48:45.957000',NULL,'2024-06-01 16:44:52.352000',0,15),(58,NULL,'2024-06-01 16:44:52.383000',NULL,'2024-06-01 16:44:52.387000',1,15);
/*!40000 ALTER TABLE `cart` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cart_detail`
--

DROP TABLE IF EXISTS `cart_detail`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `cart_detail` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `createdby` varchar(255) DEFAULT NULL,
  `createddate` datetime(6) DEFAULT NULL,
  `modifiedby` varchar(255) DEFAULT NULL,
  `modifieddate` datetime(6) DEFAULT NULL,
  `qty` int DEFAULT NULL,
  `cart_id` bigint DEFAULT NULL,
  `painting_id` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FKrg4yopd2252nwj8bfcgq5f4jp` (`cart_id`),
  KEY `FKbih0j7nqli7idag8exjrf5mta` (`painting_id`),
  CONSTRAINT `FKbih0j7nqli7idag8exjrf5mta` FOREIGN KEY (`painting_id`) REFERENCES `painting` (`id`),
  CONSTRAINT `FKrg4yopd2252nwj8bfcgq5f4jp` FOREIGN KEY (`cart_id`) REFERENCES `cart` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=121 DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cart_detail`
--

LOCK TABLES `cart_detail` WRITE;
/*!40000 ALTER TABLE `cart_detail` DISABLE KEYS */;
INSERT INTO `cart_detail` VALUES (38,NULL,'2024-05-21 21:06:54.569000',NULL,'2024-05-21 21:08:01.046000',9,8,41),(39,NULL,'2024-05-21 21:06:57.815000',NULL,'2024-05-21 21:08:11.400000',2,8,46),(40,NULL,'2024-05-21 21:07:01.972000',NULL,'2024-05-21 21:08:14.669000',7,8,48),(41,NULL,'2024-05-21 21:22:45.606000',NULL,'2024-05-22 01:04:06.944000',3,14,42),(42,NULL,'2024-05-21 21:22:46.787000',NULL,'2024-05-21 21:23:01.268000',5,14,46),(43,NULL,'2024-05-21 21:22:49.499000',NULL,'2024-05-21 21:23:06.473000',4,14,41),(44,NULL,'2024-05-21 21:22:50.260000',NULL,'2024-05-21 21:23:08.554000',9,14,48),(46,NULL,'2024-05-22 09:29:26.113000',NULL,'2024-05-22 09:29:33.311000',4,15,47),(47,NULL,'2024-05-22 09:32:04.880000',NULL,'2024-05-22 09:32:09.729000',3,16,48),(48,NULL,'2024-05-22 10:55:57.969000',NULL,'2024-05-22 10:56:03.735000',2,17,42),(49,NULL,'2024-05-22 13:54:08.360000',NULL,'2024-05-22 13:54:16.425000',5,18,46),(50,NULL,'2024-05-22 13:54:09.578000',NULL,'2024-05-22 13:54:09.583000',1,18,45),(51,NULL,'2024-05-22 13:54:10.940000',NULL,'2024-05-22 13:54:10.956000',1,18,41),(52,NULL,'2024-05-22 14:00:39.278000',NULL,'2024-05-22 14:00:44.735000',4,19,50),(53,NULL,'2024-05-22 14:00:52.777000',NULL,'2024-05-22 14:00:58.969000',6,19,43),(54,NULL,'2024-05-22 16:48:41.100000',NULL,'2024-05-22 16:48:51.399000',5,20,42),(55,NULL,'2024-05-22 16:48:42.103000',NULL,'2024-05-22 16:48:42.106000',1,20,46),(56,NULL,'2024-05-22 16:48:43.011000',NULL,'2024-05-22 16:48:43.016000',1,20,45),(57,NULL,'2024-05-22 16:48:43.980000',NULL,'2024-05-22 16:48:43.984000',1,20,41),(58,NULL,'2024-05-22 16:48:45.145000',NULL,'2024-05-22 16:48:45.150000',1,20,43),(59,NULL,'2024-05-22 16:54:51.042000',NULL,'2024-05-22 16:54:51.056000',1,22,41),(60,NULL,'2024-05-22 16:54:52.730000',NULL,'2024-05-22 16:54:52.733000',1,22,48),(61,NULL,'2024-05-22 16:59:13.422000',NULL,'2024-05-22 16:59:24.773000',7,23,43),(62,NULL,'2024-05-22 16:59:15.731000',NULL,'2024-05-22 16:59:31.170000',5,23,46),(63,NULL,'2024-05-22 16:59:18.820000',NULL,'2024-05-22 16:59:29.645000',10,23,47),(64,NULL,'2024-05-22 16:59:47.698000',NULL,'2024-05-22 16:59:55.278000',5,24,48),(65,NULL,'2024-05-22 17:39:08.802000',NULL,'2024-05-22 17:39:21.522000',6,25,46),(66,NULL,'2024-05-22 17:39:08.903000',NULL,'2024-05-22 17:39:22.744000',6,25,46),(67,NULL,'2024-05-22 17:39:12.760000',NULL,'2024-05-22 17:39:23.918000',4,25,45),(68,NULL,'2024-05-22 17:39:13.734000',NULL,'2024-05-22 17:39:24.731000',2,25,42),(69,NULL,'2024-05-22 17:44:32.264000',NULL,'2024-05-22 17:44:44.748000',6,27,51),(70,NULL,'2024-05-22 17:44:35.195000',NULL,'2024-05-22 17:44:49.840000',1,27,50),(71,NULL,'2024-05-22 17:44:36.212000',NULL,'2024-05-22 17:44:52.664000',4,27,42),(72,NULL,'2024-05-22 17:46:05.514000',NULL,'2024-05-22 17:46:05.518000',1,28,43),(73,NULL,'2024-05-22 17:46:08.689000',NULL,'2024-05-22 17:46:08.693000',1,28,48),(74,NULL,'2024-05-22 17:47:27.836000',NULL,'2024-05-22 17:47:33.806000',9,29,46),(75,NULL,'2024-05-22 22:05:31.153000',NULL,'2024-05-22 22:05:41.072000',6,21,46),(76,NULL,'2024-05-22 22:05:32.317000',NULL,'2024-05-22 22:05:42.219000',5,21,45),(77,NULL,'2024-05-22 22:05:33.324000',NULL,'2024-05-22 22:05:33.328000',1,21,42),(78,NULL,'2024-05-22 22:05:35.067000',NULL,'2024-05-22 22:05:35.080000',1,21,41),(79,NULL,'2024-05-23 08:01:06.620000',NULL,'2024-05-23 08:01:32.399000',3,30,41),(80,NULL,'2024-05-23 08:01:07.826000',NULL,'2024-05-23 08:01:33.595000',4,30,51),(81,NULL,'2024-05-23 08:01:08.889000',NULL,'2024-05-23 08:01:34.514000',3,30,50),(82,NULL,'2024-05-28 07:28:20.446000',NULL,'2024-05-28 07:28:27.706000',9,31,51),(83,NULL,'2024-05-28 07:35:11.091000',NULL,'2024-05-28 07:35:16.941000',6,33,41),(84,NULL,'2024-05-28 07:36:53.898000',NULL,'2024-05-28 07:36:53.902000',1,34,46),(85,NULL,'2024-05-28 07:49:01.057000',NULL,'2024-05-28 07:49:01.060000',1,35,51),(86,NULL,'2024-05-28 08:05:12.882000',NULL,'2024-05-28 08:05:12.896000',1,36,51),(87,NULL,'2024-05-28 16:12:34.127000',NULL,'2024-05-28 16:14:31.572000',2,37,51),(88,NULL,'2024-05-28 17:39:42.090000',NULL,'2024-05-28 17:39:47.291000',7,38,41),(89,NULL,'2024-05-28 17:41:45.225000',NULL,'2024-05-28 17:41:50.952000',4,39,51),(90,NULL,'2024-05-28 17:43:33.051000',NULL,'2024-05-28 17:43:33.054000',1,40,41),(91,NULL,'2024-05-28 17:43:34.338000',NULL,'2024-05-28 17:43:34.352000',1,40,51),(92,NULL,'2024-05-28 17:44:35.260000',NULL,'2024-05-28 17:44:35.276000',1,41,51),(93,NULL,'2024-05-28 17:44:52.955000',NULL,'2024-05-28 17:44:52.958000',1,41,43),(94,NULL,'2024-05-28 17:48:47.610000',NULL,'2024-05-28 17:48:47.614000',1,42,41),(95,NULL,'2024-05-28 17:48:48.339000',NULL,'2024-05-28 17:48:48.343000',1,42,51),(96,NULL,'2024-05-28 17:48:49.828000',NULL,'2024-05-28 17:48:49.831000',1,42,42),(97,NULL,'2024-05-28 17:49:17.538000',NULL,'2024-05-28 17:49:17.541000',1,43,41),(98,NULL,'2024-05-28 17:49:18.897000',NULL,'2024-05-28 17:49:18.899000',1,43,51),(99,NULL,'2024-05-28 17:51:51.255000',NULL,'2024-05-28 17:51:51.257000',1,44,51),(100,NULL,'2024-05-28 17:56:11.092000',NULL,'2024-05-28 17:56:11.094000',1,45,51),(101,NULL,'2024-05-28 18:37:33.383000',NULL,'2024-05-28 18:37:40.043000',4,46,41),(102,NULL,'2024-05-28 18:37:34.356000',NULL,'2024-05-28 18:37:40.904000',4,46,51),(103,NULL,'2024-05-29 06:33:48.529000',NULL,'2024-05-29 06:33:54.182000',5,47,41),(104,NULL,'2024-05-29 06:57:19.397000',NULL,'2024-05-29 06:57:29.838000',25,48,51),(105,NULL,'2024-05-29 07:12:27.110000',NULL,'2024-05-29 07:12:33.323000',4,49,43),(106,NULL,'2024-05-29 07:12:27.956000',NULL,'2024-05-29 07:12:33.930000',2,49,51),(107,NULL,'2024-05-29 08:44:20.944000',NULL,'2024-05-29 08:44:27.766000',5,50,50),(108,NULL,'2024-05-29 08:44:22.434000',NULL,'2024-05-29 08:44:28.918000',4,50,51),(109,NULL,'2024-05-29 08:45:21.971000',NULL,'2024-05-29 08:45:21.989000',1,51,50),(110,NULL,'2024-05-29 08:45:24.212000',NULL,'2024-05-29 08:45:31.940000',12,51,51),(111,NULL,'2024-05-29 08:51:22.249000',NULL,'2024-05-29 08:51:39.006000',14,52,45),(112,NULL,'2024-05-29 08:51:25.427000',NULL,'2024-05-29 08:51:35.324000',18,52,46),(113,NULL,'2024-05-29 08:52:09.118000',NULL,'2024-05-29 08:52:20.331000',17,53,45),(114,NULL,'2024-05-29 10:12:50.026000',NULL,'2024-05-29 10:12:50.124000',1,54,41),(115,NULL,'2024-05-29 10:12:52.278000',NULL,'2024-05-29 10:12:58.839000',9,54,46),(116,NULL,'2024-05-29 10:13:20.031000',NULL,'2024-05-29 10:13:29.155000',26,55,46),(117,NULL,'2024-05-29 10:22:01.143000',NULL,'2024-05-29 10:22:10.702000',28,56,45),(118,NULL,'2024-06-01 16:01:26.116000',NULL,'2024-06-01 16:01:26.140000',1,57,41),(119,NULL,'2024-06-01 16:01:27.655000',NULL,'2024-06-01 16:01:27.671000',1,57,51),(120,NULL,'2024-06-01 16:01:30.256000',NULL,'2024-06-01 16:01:30.262000',1,57,50);
/*!40000 ALTER TABLE `cart_detail` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `detail_received_log`
--

DROP TABLE IF EXISTS `detail_received_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `detail_received_log` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `createdby` varchar(255) DEFAULT NULL,
  `createddate` datetime(6) DEFAULT NULL,
  `modifiedby` varchar(255) DEFAULT NULL,
  `modifieddate` datetime(6) DEFAULT NULL,
  `qty` int DEFAULT NULL,
  `unit_price` double DEFAULT NULL,
  `painting_id` bigint DEFAULT NULL,
  `received_log_id` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `FKilxkusbjtadlgf86cwo1n224o` (`painting_id`),
  KEY `FKqpq9bn12dghy18v1x4xow7pl4` (`received_log_id`),
  CONSTRAINT `FKilxkusbjtadlgf86cwo1n224o` FOREIGN KEY (`painting_id`) REFERENCES `painting` (`id`),
  CONSTRAINT `FKqpq9bn12dghy18v1x4xow7pl4` FOREIGN KEY (`received_log_id`) REFERENCES `received_log` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `detail_received_log`
--

LOCK TABLES `detail_received_log` WRITE;
/*!40000 ALTER TABLE `detail_received_log` DISABLE KEYS */;
/*!40000 ALTER TABLE `detail_received_log` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `orders`
--

DROP TABLE IF EXISTS `orders`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `orders` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `createdby` varchar(255) DEFAULT NULL,
  `createddate` datetime(6) DEFAULT NULL,
  `modifiedby` varchar(255) DEFAULT NULL,
  `modifieddate` datetime(6) DEFAULT NULL,
  `cancellation_date` datetime(6) DEFAULT NULL,
  `code` varchar(255) DEFAULT NULL,
  `delivery_date` datetime(6) DEFAULT NULL,
  `finished_date` datetime(6) DEFAULT NULL,
  `order_date` datetime(6) DEFAULT NULL,
  `status` int DEFAULT NULL,
  `cart_id` bigint DEFAULT NULL,
  `delivery_address` varchar(255) DEFAULT NULL,
  `payment_status` int DEFAULT NULL,
  `shipping_cost` double DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_s1sr8a1rkx80gwq9pl0952dar` (`cart_id`),
  CONSTRAINT `FKtpihbdn6ws0hu56camb0bg2to` FOREIGN KEY (`cart_id`) REFERENCES `cart` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=50 DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `orders`
--

LOCK TABLES `orders` WRITE;
/*!40000 ALTER TABLE `orders` DISABLE KEYS */;
INSERT INTO `orders` VALUES (7,NULL,'2024-04-20 16:35:08.724000',NULL,'2024-05-22 18:09:59.352000','2024-05-22 18:09:59.352000','ORD-20240420-7',NULL,NULL,'2024-04-20 16:35:08.743000',0,10,NULL,0,NULL),(8,NULL,'2024-05-21 21:11:20.528000',NULL,'2024-05-22 18:10:03.281000',NULL,'ORD-20240521-8','2024-05-22 10:33:01.850000','2024-05-22 18:10:03.281000','2024-05-21 21:11:20.532000',3,8,NULL,0,NULL),(9,NULL,'2024-05-22 09:28:12.735000',NULL,'2024-05-22 18:10:04.004000',NULL,'ORD-20240522-9','2024-05-22 10:42:47.850000','2024-05-22 18:10:04.005000','2024-05-22 09:28:12.757000',3,14,NULL,0,0),(10,NULL,'2024-05-22 09:29:38.838000',NULL,'2024-05-22 13:53:38.663000',NULL,'ORD-20240522-10',NULL,'2024-05-22 13:53:38.668000','2024-05-22 09:29:38.839000',3,15,NULL,0,0),(11,NULL,'2024-05-22 09:32:18.790000',NULL,'2024-05-22 11:29:37.218000',NULL,'ORD-20240522-11','2024-05-22 11:29:12.219000','2024-05-22 11:29:37.218000','2024-05-22 09:32:18.792000',3,16,NULL,0,5),(12,NULL,'2024-05-22 12:51:45.559000',NULL,'2024-05-22 13:50:29.406000',NULL,'ORD-20240522-12',NULL,'2024-05-22 13:50:29.406000','2024-05-22 12:51:45.569000',3,17,NULL,0,0),(13,NULL,'2024-05-22 13:54:18.332000',NULL,'2024-05-22 13:56:14.461000','2024-05-22 13:56:14.485000','ORD-20240522-13',NULL,NULL,'2024-05-22 13:54:18.332000',0,18,NULL,0,0),(14,NULL,'2024-05-22 14:01:00.706000',NULL,'2024-05-22 16:48:17.252000',NULL,'ORD-20240522-14','2024-05-22 16:48:12.762000','2024-05-22 16:48:17.252000','2024-05-22 14:01:00.710000',3,19,NULL,0,0),(15,NULL,'2024-05-22 16:48:52.260000',NULL,'2024-05-22 18:10:36.499000',NULL,'ORD-20240522-15','2024-05-22 16:49:05.818000','2024-05-22 18:10:36.501000','2024-05-22 16:48:52.260000',3,20,NULL,0,0),(16,NULL,'2024-05-22 16:54:56.275000',NULL,'2024-05-22 16:58:16.315000',NULL,'ORD-20240522-16','2024-05-22 16:55:17.014000','2024-05-22 16:58:16.315000','2024-05-22 16:54:56.275000',3,22,NULL,0,0),(17,NULL,'2024-05-22 16:59:34.091000',NULL,'2024-05-22 18:10:34.534000',NULL,'ORD-20240522-17','2024-05-22 16:59:37.473000','2024-05-22 18:10:34.534000','2024-05-22 16:59:34.098000',3,23,NULL,0,0),(18,NULL,'2024-05-22 16:59:57.444000',NULL,'2024-05-22 17:00:02.281000','2024-05-22 17:00:02.281000','ORD-20240522-18',NULL,NULL,'2024-05-22 16:59:57.444000',0,24,NULL,0,0),(19,NULL,'2024-05-22 17:39:26.522000',NULL,'2024-05-22 17:39:44.076000',NULL,'ORD-20240522-19','2024-05-22 17:39:41.201000','2024-05-22 17:39:44.076000','2024-05-22 17:39:26.525000',3,25,NULL,0,0),(20,NULL,'2024-05-22 17:45:09.831000',NULL,'2024-05-22 17:45:57.009000',NULL,'ORD-20240522-20','2024-05-22 17:45:51.212000','2024-05-22 17:45:57.009000','2024-05-22 17:45:09.831000',3,27,NULL,0,0),(21,NULL,'2024-05-22 17:46:12.301000',NULL,'2024-05-22 17:46:18.078000','2024-05-22 17:46:18.079000','ORD-20240522-21',NULL,NULL,'2024-05-22 17:46:12.301000',0,28,NULL,0,0),(22,NULL,'2024-05-22 17:47:34.731000',NULL,'2024-05-22 17:48:02.895000',NULL,'ORD-20240522-22','2024-05-22 17:47:38.684000','2024-05-22 17:48:02.895000','2024-05-22 17:47:34.732000',3,29,NULL,0,0),(23,NULL,'2024-05-22 22:05:42.903000',NULL,'2024-05-22 22:06:44.911000',NULL,'ORD-20240522-23','2024-05-22 22:06:44.911000',NULL,'2024-05-22 22:05:42.903000',2,21,NULL,0,0),(24,NULL,'2024-05-23 08:01:37.501000',NULL,'2024-05-23 08:01:37.517000',NULL,'ORD-20240523-24',NULL,NULL,'2024-05-23 08:01:37.501000',1,30,NULL,0,0),(25,NULL,'2024-05-28 07:28:28.840000',NULL,'2024-05-28 07:28:49.830000',NULL,'ORD-20240528-25','2024-05-28 07:28:48.907000','2024-05-28 07:28:49.830000','2024-05-28 07:28:28.844000',3,31,NULL,0,0),(26,NULL,'2024-05-28 07:35:17.590000',NULL,'2024-05-28 07:35:17.594000',NULL,'ORD-20240528-26',NULL,NULL,'2024-05-28 07:35:17.590000',1,33,NULL,0,0),(27,NULL,'2024-05-28 07:36:57.406000',NULL,'2024-05-28 07:36:57.411000',NULL,'ORD-20240528-27',NULL,NULL,'2024-05-28 07:36:57.406000',1,34,NULL,0,0),(28,NULL,'2024-05-28 07:49:07.054000',NULL,'2024-05-28 07:49:07.069000',NULL,'ORD-20240528-28',NULL,NULL,'2024-05-28 07:49:07.054000',1,35,NULL,0,0),(29,NULL,'2024-05-28 08:05:16.174000',NULL,'2024-05-28 08:05:16.187000',NULL,'ORD-20240528-29',NULL,NULL,'2024-05-28 08:05:16.174000',1,36,NULL,0,0),(30,NULL,'2024-05-28 16:14:32.964000',NULL,'2024-05-28 16:15:34.546000',NULL,'ORD-20240528-30','2024-05-28 16:15:33.842000','2024-05-28 16:15:34.546000','2024-05-28 16:14:32.967000',3,37,NULL,0,0),(31,NULL,'2024-05-28 17:39:48.423000',NULL,'2024-05-28 17:39:48.428000',NULL,'ORD-20240528-31',NULL,NULL,'2024-05-28 17:39:48.423000',1,38,NULL,0,0),(32,NULL,'2024-05-28 17:41:53.215000',NULL,'2024-05-28 17:41:53.231000',NULL,'ORD-20240528-32',NULL,NULL,'2024-05-28 17:41:53.215000',1,39,NULL,0,0),(33,NULL,'2024-05-28 17:43:37.461000',NULL,'2024-05-28 17:43:37.466000',NULL,'ORD-20240528-33',NULL,NULL,'2024-05-28 17:43:37.461000',1,40,NULL,0,0),(34,NULL,'2024-05-28 17:44:56.581000',NULL,'2024-05-28 17:44:56.586000',NULL,'ORD-20240528-34',NULL,NULL,'2024-05-28 17:44:56.581000',1,41,NULL,0,0),(35,NULL,'2024-05-28 17:48:53.898000',NULL,'2024-05-28 17:48:53.903000',NULL,'ORD-20240528-35',NULL,NULL,'2024-05-28 17:48:53.898000',1,42,NULL,0,0),(36,NULL,'2024-05-28 17:49:22.889000',NULL,'2024-05-28 17:50:35.618000',NULL,'ORD-20240528-36','2024-05-28 17:49:54.829000','2024-05-28 17:50:35.618000','2024-05-28 17:49:22.889000',3,43,NULL,0,0),(37,NULL,'2024-05-28 17:51:54.592000',NULL,'2024-05-28 17:55:28.941000',NULL,'ORD-20240528-37','2024-05-28 17:52:09.147000','2024-05-28 17:55:28.941000','2024-05-28 17:51:54.592000',3,44,NULL,0,0),(38,NULL,'2024-05-28 17:56:14.314000',NULL,'2024-05-28 18:37:18.362000',NULL,'ORD-20240528-38','2024-05-28 18:37:05.356000','2024-05-28 18:37:18.362000','2024-05-28 17:56:14.314000',3,45,NULL,0,0),(39,NULL,'2024-05-28 18:37:43.036000',NULL,'2024-05-28 18:38:29.996000','2024-05-28 18:38:29.996000','ORD-20240528-39',NULL,NULL,'2024-05-28 18:37:43.036000',0,46,NULL,0,0),(40,NULL,'2024-05-29 06:56:57.386000',NULL,'2024-05-29 06:59:48.413000','2024-05-29 06:59:48.413000','ORD-20240529-40',NULL,NULL,'2024-05-29 06:56:57.386000',0,47,NULL,0,0),(41,NULL,'2024-05-29 06:57:30.904000',NULL,'2024-05-29 06:59:01.639000','2024-05-29 06:59:01.639000','ORD-20240529-41',NULL,NULL,'2024-05-29 06:57:30.904000',0,48,NULL,0,0),(42,NULL,'2024-05-29 07:12:35.908000',NULL,'2024-05-29 07:15:28.076000','2024-05-29 07:15:28.076000','ORD-20240529-42','2024-05-29 07:15:23.901000',NULL,'2024-05-29 07:12:35.908000',0,49,NULL,0,0),(43,NULL,'2024-05-29 08:44:39.951000',NULL,'2024-05-29 08:44:39.951000',NULL,'ORD-20240529-43',NULL,NULL,'2024-05-29 08:44:39.951000',1,50,NULL,0,0),(44,NULL,'2024-05-29 08:45:32.822000',NULL,'2024-05-29 08:45:32.826000',NULL,'ORD-20240529-44',NULL,NULL,'2024-05-29 08:45:32.823000',1,51,NULL,0,0),(45,NULL,'2024-05-29 08:51:40.418000',NULL,'2024-05-29 08:51:40.423000',NULL,'ORD-20240529-45',NULL,NULL,'2024-05-29 08:51:40.418000',1,52,NULL,0,0),(46,NULL,'2024-05-29 08:52:20.360000',NULL,'2024-06-01 16:48:00.729000','2024-06-01 16:48:00.729000','ORD-20240529-46','2024-06-01 16:36:55.154000',NULL,'2024-05-29 08:52:20.361000',0,53,NULL,0,0),(47,NULL,'2024-05-29 10:13:00.146000',NULL,'2024-06-01 16:36:55.667000',NULL,'ORD-20240529-47','2024-06-01 16:36:54.620000','2024-06-01 16:36:55.667000','2024-05-29 10:13:00.146000',3,54,NULL,0,0),(48,NULL,'2024-05-29 10:13:30.197000',NULL,'2024-06-01 16:36:56.076000',NULL,'ORD-20240529-48','2024-06-01 16:36:53.984000','2024-06-01 16:36:56.076000','2024-05-29 10:13:30.197000',3,55,NULL,0,0),(49,NULL,'2024-06-01 16:44:52.338000',NULL,'2024-06-01 16:47:38.519000',NULL,'ORD-20240601-49','2024-06-01 16:47:09.883000','2024-06-01 16:47:38.520000','2024-06-01 16:44:52.338000',3,57,NULL,0,0);
/*!40000 ALTER TABLE `orders` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `painting`
--

DROP TABLE IF EXISTS `painting`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `painting` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `createdby` varchar(255) DEFAULT NULL,
  `createddate` datetime(6) DEFAULT NULL,
  `modifiedby` varchar(255) DEFAULT NULL,
  `modifieddate` datetime(6) DEFAULT NULL,
  `code` varchar(255) DEFAULT NULL,
  `inventory` int DEFAULT NULL,
  `length` double DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `price` double DEFAULT NULL,
  `thickness` double DEFAULT NULL,
  `thumbnail_url` varchar(255) DEFAULT NULL,
  `width` double DEFAULT NULL,
  `artist` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=52 DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `painting`
--

LOCK TABLES `painting` WRITE;
/*!40000 ALTER TABLE `painting` DISABLE KEYS */;
INSERT INTO `painting` VALUES (41,NULL,NULL,NULL,'2024-05-23 07:56:30.208000','PAW-41',51,55,'Portrait PS 237 Still Storm LA French School Artist Affordable Print',56.4,3.5,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716106842/10403509-ZFMIZJNU-6.jpg',55,NULL),(42,NULL,NULL,NULL,'2024-05-19 15:40:11.280000','PAW-42',45,152.4,'Last Minute',910,2,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107116/2820411-HSC00001-6.jpg',101.6,NULL),(43,NULL,NULL,NULL,'2024-05-28 18:20:48.169000','PAW-43',100,101.6,'A Photographer in Paris',6.9,38.1,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107242/1995066-HSC00001-6.jpg',76.2,NULL),(44,NULL,'2024-05-19 15:30:02.369000',NULL,'2024-05-19 15:30:02.384000','PAW-44',7,70,'Various',3.47,2,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107397/7321484-LGKDDESD-6.jpg',80,NULL),(45,NULL,NULL,NULL,'2024-05-19 15:39:55.210000','PAW-45',45,101.6,'Hair',920,2,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107561/10419515-GZGAKLNT-7.jpg',101.6,NULL),(46,NULL,NULL,NULL,'2024-05-19 15:40:01.620000','PAW-46',995,30.5,'Warmth',995,2.5,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107691/10580977-YRBSBJKZ-7.jpg',30.5,NULL),(47,NULL,NULL,NULL,'2024-05-19 15:39:31.383000','PAW-47',45,91.4,'Country View',4.11,2.5,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107777/10220045-AYIXZUEN-7.jpg',152.4,NULL),(48,NULL,NULL,NULL,'2024-05-19 15:39:35.221000','PAW-48',945,80,'Fortuity, the king of life choices',2.55,2,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1716107930/2060217-JIWWEIBB-7.jpg',90,NULL),(50,NULL,NULL,NULL,'2024-05-22 14:00:27.905000','PAW-50',3,NULL,'NOOK_A360',39.6,NULL,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1712747040/8720961-MIODUCGC-6.jpg',NULL,NULL),(51,NULL,'2024-05-22 17:42:16.368000',NULL,'2024-05-22 17:42:16.400000','PAW-51',10000,91.4,'Mona Lisa',123,2.5,'http://res.cloudinary.com/dzbiwncwe/image/upload/v1712762976/Mona_Lisa.jpg',30.5,NULL);
/*!40000 ALTER TABLE `painting` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `painting_topic`
--

DROP TABLE IF EXISTS `painting_topic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `painting_topic` (
  `painting_id` bigint NOT NULL,
  `topic_id` bigint NOT NULL,
  PRIMARY KEY (`painting_id`,`topic_id`),
  KEY `FK7o97ksccbmm3cxbjao1ewoug2` (`topic_id`),
  CONSTRAINT `FK7o97ksccbmm3cxbjao1ewoug2` FOREIGN KEY (`topic_id`) REFERENCES `topic` (`id`),
  CONSTRAINT `FKm76k4h5pfoi3ewmpdn3jcon93` FOREIGN KEY (`painting_id`) REFERENCES `painting` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `painting_topic`
--

LOCK TABLES `painting_topic` WRITE;
/*!40000 ALTER TABLE `painting_topic` DISABLE KEYS */;
INSERT INTO `painting_topic` VALUES (41,21),(42,21),(45,21),(46,21),(47,21),(48,21),(50,21),(51,21),(43,24),(50,24),(51,24),(41,25),(42,25),(43,25),(44,25),(45,25),(46,25),(41,26),(47,26),(48,26),(50,26),(51,26);
/*!40000 ALTER TABLE `painting_topic` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `received_log`
--

DROP TABLE IF EXISTS `received_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `received_log` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `createdby` varchar(255) DEFAULT NULL,
  `createddate` datetime(6) DEFAULT NULL,
  `modifiedby` varchar(255) DEFAULT NULL,
  `modifieddate` datetime(6) DEFAULT NULL,
  `code` varchar(255) DEFAULT NULL,
  `date_added` datetime(6) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `received_log`
--

LOCK TABLES `received_log` WRITE;
/*!40000 ALTER TABLE `received_log` DISABLE KEYS */;
INSERT INTO `received_log` VALUES (1,NULL,'2024-04-15 16:57:54.321000',NULL,'2024-04-15 16:57:54.443000','PN_20240415-1',NULL,NULL),(2,NULL,'2024-04-15 16:58:51.466000',NULL,'2024-04-15 16:58:51.478000','PN_20240415-2',NULL,NULL),(3,NULL,'2024-04-15 17:33:13.506000',NULL,'2024-04-15 17:33:13.623000','PN_20240415-3',NULL,NULL),(4,NULL,'2024-04-15 17:34:38.229000',NULL,'2024-04-15 17:34:58.011000','PN_20240415-4',NULL,NULL);
/*!40000 ALTER TABLE `received_log` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `topic`
--

DROP TABLE IF EXISTS `topic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `topic` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `createdby` varchar(255) DEFAULT NULL,
  `createddate` datetime(6) DEFAULT NULL,
  `modifiedby` varchar(255) DEFAULT NULL,
  `modifieddate` datetime(6) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `title` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=utf8mb3;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `topic`
--

LOCK TABLES `topic` WRITE;
/*!40000 ALTER TABLE `topic` DISABLE KEYS */;
INSERT INTO `topic` VALUES (21,NULL,'2024-05-19 15:03:49.661000',NULL,'2024-05-19 15:03:49.661000','','Drawings'),(24,NULL,'2024-05-19 15:14:39.503000',NULL,'2024-05-19 15:14:39.503000','','Photography'),(25,NULL,'2024-05-19 15:14:50.647000',NULL,'2024-05-19 15:14:50.647000','','Paintings'),(26,NULL,'2024-05-19 15:39:29.165000',NULL,'2024-05-19 15:39:29.165000','','Country');
/*!40000 ALTER TABLE `topic` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-06-01 17:54:52
