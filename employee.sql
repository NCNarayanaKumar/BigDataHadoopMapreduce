create database if not exists kalyan_test_db;

use kalyan_test_db;

CREATE TABLE  if not exists employee ( id INT, name VARCHAR(50), dept VARCHAR(15), salary DOUBLE);

INSERT INTO employee (id, name, dept, salary) VALUES (1, 'Kalyan', 'Dev', 11111);

INSERT INTO employee (id, name, dept, salary) VALUES (2, 'Venkat', 'Dev', 22222);

INSERT INTO employee (id, name, dept, salary) VALUES (3, 'Kumar', 'Test', 33333);

INSERT INTO employee (id, name, dept, salary) VALUES (4, 'Anil', 'Test', 44444);

select * from employee;

create table employee_export like employee;



