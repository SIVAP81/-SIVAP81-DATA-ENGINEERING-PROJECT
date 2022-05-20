/*connect to mysql server*/
mysql -uanabig114222 -p

/*displaying the databases in the mysql server */
show databases;

/*using the databases */
use anabig114222 

/*displaying the tables in the database */
show tables;

/*droping the tables if exists in the databases*/

drop table if exists employees;
drop table if exists departments;
drop table if exists dept_emp;
drop table if exists dept_manager;
drop table if exists titles;
drop table if exists salaries;

/*creating the employees table in the database */
create table employees(emp_no int not null,emp_title_id varchar(25),birth_date varchar(10),first_name varchar(25),last_name varchar(25),sex char,hire_date varchar(10),no_of_projects int,Last_performance_rating char(3),left_company int,last_date varchar(10));

/*creating the departments table in the database */
create table departments(dept_no varchar(10),dept_name varchar(25));

/*creating the dept_emp table in the database */
create table dept_emp(emp_no int ,dept_no varchar(10));

/*creating the dept_manager table in the database */
create table dept_manager(dept_no varchar(10),emp_no int);

/*creating the titles table in the database */
create table titles(title_id varchar(10),title varchar(10));

/*creating the salaries table in the database */
create table salaries(emp_no int ,salary float);


/* adding the primary key and foreign key to the tables */
alter table titles add primary key(title_id);
alter table employees ADD PRIMARY KEY(emp_no);
alter table employees add foreign key(emp_title_id) REFERENCES titles(title_id);
alter table salaries add foreign key(emp_no) REFERENCES employees(emp_no);
alter table departments add primary key(dept_no);
alter table dept_emp add foreign key(emp_no) REFERENCES employees(emp_no);
alter table dept_emp add foreign key(dept_no) REFERENCES departments(dept_no);
alter table dept_manager add foreign key(emp_no) REFERENCES employees(emp_no);
alter table dept_manager add foreign key(dept_no) REFERENCES departments(dept_no);

/*displaying the tables structure */
describe employees;
describe departments;
describe dept_emp;
describe dept_manager;
describe titles;
describe salaries;


/* saving data in the our local system and then loading the data into mysql tables *
LOAD DATA LOCAL INFILE '/home/anabig114222/Data/employees.csv' INTO TABLE employees 
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ignore 1 rows;

LOAD DATA LOCAL INFILE '/home/anabig114222/Data/employees.csv' INTO TABLE titles
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ignore 1 rows;

LOAD DATA LOCAL INFILE '/home/anabig114222/Data/departments.csv' INTO TABLE departments
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ignore 1 rows;

LOAD DATA LOCAL INFILE '/home/anabig114222/Data/dept_manager.csv' INTO TABLE dept_manager
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ignore 1 rows;

LOAD DATA LOCAL INFILE '/home/anabig114222/Data/salaries.csv' INTO TABLE salaries
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ignore 1 rows;

LOAD DATA LOCAL INFILE '/home/anabig114222/Data/dept_emp.csv' INTO TABLE dept_emp
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ignore 1 rows;

/* displaying the tables */

select * from employees limit 10;
select * from dept_manager limit 10;
select * from dept_emp limit 10;
select * from departments limit 10;
select * from titles limit 10;
select * from salaries limit 10;

!quit