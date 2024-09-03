CREATE USER 'replica'@'%' IDENTIFIED BY 'password';
GRANT REPLICATION SLAVE ON *.* TO 'replica'@'%';
FLUSH PRIVILEGES;

CREATE TABLE table1 (
    orderId INT PRIMARY KEY,
    orderItem VARCHAR(255),
    customerName VARCHAR(255),
    orderDate DATE
);

INSERT INTO table1 (orderId, orderItem, customerName, orderDate)
VALUES
(1, 'Item A', 'John Doe', '2024-08-01'),
(2, 'Item B', 'Jane Smith', '2024-08-05');

CREATE TABLE table2 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    orderId INT,
    orderStatus INT CHECK (orderStatus IN (1, 2, 3, 4, 5)),
    addDate DATE,
    FOREIGN KEY (orderId) REFERENCES table1(orderId)
);

INSERT INTO table2 (orderId, orderStatus, addDate)
VALUES
(1, 1, '2024-08-01'),
(1, 2, '2024-08-10'),
(2, 3, '2024-08-06');

CREATE TABLE `table3` (
  `orderId` int NOT NULL,
  `orderItem` varchar(255) DEFAULT NULL,
  `customerName` varchar(255) DEFAULT NULL,
  `orderDate` varchar(255) DEFAULT NULL,
  `orderStatus1` varchar(255) DEFAULT '0',
  `orderStatus2` varchar(255) DEFAULT '0',
  `orderStatus3` varchar(255) DEFAULT '0',
  `orderStatus4` varchar(255) DEFAULT '0',
  `orderStatus5` varchar(255) DEFAULT '0',
  UNIQUE KEY `orderId` (`orderId`)
) ;