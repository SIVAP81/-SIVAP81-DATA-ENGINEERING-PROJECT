/*using the hive shell */

hive

/* createing a database in hive:

CREATE DATABASE if not exists CAPSTONEPROJECT;

/* use databases CAPSTONEPROJECT */

USE CAPSTONEPROJECT;

drop table if exists departments;
drop table if exists dept_manager;
drop table if exists dept_emp;
drop table if exists salaries;
drop table if exists titles;
drop table if exists employees;

/*loading the avrofiles data into tables in the databases */ 

CREATE TABLE departments STORED AS AVRO LOCATION 'hdfs:///user/anabig114222/capstoneproject/warehouse/departments' TBLPROPERTIES ('avro.schema.url'='/user/anabig114222/departments.avsc');

select * from departments limit 10;

CREATE  TABLE dept_manager
STORED AS AVRO LOCATION 'hdfs:///user/anabig114222/capstoneproject/warehouse/dept_manager'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114222/dept_manager.avsc');

select * from dept_manager limit 6;

CREATE  TABLE dept_emp
STORED AS AVRO LOCATION 'hdfs:///user/anabig114222/capstoneproject/warehouse/dept_emp'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114222/dept_emp.avsc');

SELECT * from dept_emp;

CREATE  TABLE employees
STORED AS AVRO LOCATION 'hdfs:///user/anabig114222/capstoneproject/warehouse/employees'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114222/employees.avsc');

SELECT * from employees LIMIT 10;

CREATE  TABLE salaries
STORED AS AVRO LOCATION 'hdfs:///user/anabig114222/capstoneproject/warehouse/salaries'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114222/salaries.avsc');

SELECT *  from salaries limit 5;

CREATE  TABLE titles
STORED AS AVRO LOCATION 'hdfs:///user/anabig114222/capstoneproject/warehouse/titles'
TBLPROPERTIES ('avro.schema.url'='/user/anabig114222/titles.avsc');

SELECT * from titles limit 5; 

/**Exploratory Data Analysis **/
/**The queries in database include**/
/**1.A list showing employee number, last name, first name, sex, and salary for each employee */

select e.emp_no,first_name,last_name, sex,salary  from  employees e INNER JOIN salaries s on e.emp_no=s.emp_no;

/**2.A list showing first name, last name, and hire date for employees who were hired in 1986.**/

select first_name,last_name,hire_date, year(date_format(from_unixtime(unix_timestamp(cast(hire_date as string),'mm/dd/yyyy')),'yyyy-MM-dd')) AS year_ FROM employees
where year(date_format(from_unixtime(unix_timestamp(cast(hire_date as string),'mm/dd/yyyy')),'yyyy-MM-dd'))=1986;

/** 3.A list showing the manager of each department with the following information: department number, department name, 
the manager's employee number, last name, first name.**/

SELECT dm.dept_no ,e.emp_no,e.last_name,e.first_name,dp.dept_name 
FROM departments dp 
INNER JOIN dept_manager dm on dp.dept_no=dm.dept_no
INNER JOIN employees e on dm.emp_no=e.emp_no;

/*4. A list showing the department of each employee with the following information: employee number, last name, first 
name, and department name.*/

SELECT e.emp_no,e.last_name,e.first_name,dp.dept_name 
FROM departments dp inner join dept_emp de on dp.dept_no=de.dept_no 
inner join employees e on de.emp_no=e.emp_no


/*5.A list showing first name, last name, and sex for employees whose first name is "Hercules" and last names begin with "B.“ */

SELECT last_name,first_name,sex FROM employees WHERE first_name= "Hercules" and last_name like 'B%'


/* 6. A list showing all employees in the Sales department, including their employee number, last name, first name, and 
department name. */
SELECT * FROM departments;
select employees.emp_no, employees.last_name, employees.first_name, departments.dept_name 
from  departments
inner join  dept_emp on departments.dept_no = dept_emp.dept_no
inner join employees on dept_emp.emp_no = employees.emp_no
where dept_name = '"Sales"';

/* 7. A list showing all employees in the Sales and Development departments, including their employee number, last name, 
first name, and department name. **/

select employees.emp_no, employees.last_name, employees.first_name, departments.dept_name 
from  departments
inner join  dept_emp on departments.dept_no = dept_emp.dept_no
inner join employees on dept_emp.emp_no = employees.emp_no
where dept_name = '"Sales"' OR dept_name= '"development"';


/*8. A list showing the frequency count of employee last names, in descending order. ( i.e., how many employees share each 
last name */

SELECT first_name, count(last_name) as fre_count FROM employees 
GROUP BY first_name
ORDER BY fre_count;


/*9. Histogram to show the salary distribution among the employees */

/*10. Bar graph to show the Average salary per title (designation) */

select t.title,avg(s.salary) as avg_salary from titles t inner join employees e on t.title_id=e.emp_title_id 
inner join salaries s on e.emp_no=s.emp_no
group by t.title;

/*11. Calculate employee tenure & show the tenure distribution among the employees */


/*Perform your own Analysis (based on the data understanding) */   
/* 12.show the total salary per title */

select t.title,sum(s.salary) as total_salary from titles t inner join employees e on t.title_id=e.emp_title_id 
inner join salaries s on e.emp_no=s.emp_no
group by t.title;

/* 12.show the Average salary per department */
SELECT d.dept_name,avg(s.salary) avg_salary FROM departments d INNER JOIN dept_emp de on d.dept_no=de.dept_no 
INNER JOIN employees e on de.emp_no=e.emp_no 
INNER JOIN salaries s on e.emp_no=s.emp_no
GROUP BY d.dept_name;

/* 12.show the total salary per department */
SELECT d.dept_name,sum(s.salary) total_salary FROM departments d INNER JOIN dept_emp de on d.dept_no=de.dept_no 
INNER JOIN employees e on de.emp_no=e.emp_no 
INNER JOIN salaries s on e.emp_no=s.emp_no
GROUP BY d.dept_name;

/* 12.show the Average salary per gender */
SELECT sex,avg(salary) FROM employees INNER JOIN salaries on employees.emp_no=salaries.emp_no
group by sex;

!quit
