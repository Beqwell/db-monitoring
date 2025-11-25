CREATE TABLE IF NOT EXISTS customers (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  email VARCHAR(128) UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  status ENUM('NEW','PAID','CANCELLED') DEFAULT 'NEW',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX (customer_id),
  FOREIGN KEY (customer_id) REFERENCES customers(id)
);

INSERT INTO customers (name, email)
VALUES ('Alice', 'alice@example.com'),
       ('Bob',   'bob@example.com'),
       ('Carol', 'carol@example.com');
