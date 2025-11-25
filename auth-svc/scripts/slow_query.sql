-- Викликати так:
-- docker compose exec -T mysql mysql -uroot -p%MYSQL_ROOT_PASSWORD% appdb < scripts/slow_query.sql
-- Симулює повільні запити (sleep) у циклі
DELIMITER //
CREATE PROCEDURE slowp()
BEGIN
  DECLARE i INT DEFAULT 0;
  WHILE i < 50 DO
    SELECT SLEEP(2);
    SET i = i + 1;
  END WHILE;
END//
DELIMITER ;
CALL slowp();
DROP PROCEDURE slowp;
