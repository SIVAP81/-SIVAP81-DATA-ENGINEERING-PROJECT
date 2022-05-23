#/** (Transfer data from MySQL Server to HDFS/Hive)*/
#/* Remove the existing data from hdfs incase if you created below tables in the past */

hdfs dfs -rm -r /user/anabig114222/hive/warehouse/employees/
hdfs dfs -rm -r /user/anabig114222/hive/warehouse/dept_manager/
hdfs dfs -rm -r /user/anabig114222/hive/warehouse/dept_emp/
hdfs dfs -rm -r /user/anabig114222/hive/warehouse/departments/
hdfs dfs -rm -r /user/anabig114222/hive/warehouse/titles/
hdfs dfs -rm -r /user/anabig114222/hive/warehouse/salaries/

#removing .avsc and .java files
rm /home/anabig114222/*.avsc
rm /home/anabig114222/*.java

#removing parquetfile 
hdfs dfs -rm -r /user/anabig114222/hive/warehouse2/employees/
hdfs dfs -rm -r /user/anabig114222/hive/warehouse2/dept_manager/
hdfs dfs -rm -r /user/anabig114222/hive/warehouse2/dept_emp/
hdfs dfs -rm -r /user/anabig114222/hive/warehouse2/departments/
hdfs dfs -rm -r /user/anabig114222/hive/warehouse2/titles/
hdfs dfs -rm -r /user/anabig114222/hive/warehouse2/salaries/

#removing .avsc user/anabig114222
hdfs dfs -rm -r /user/anabig114222/employees.avsc
hdfs dfs -rm -r /user/anabig114222/dept_manager.avsc
hdfs dfs -rm -r /user/anabig114222/dept_emp.avsc
hdfs dfs -rm -r /user/anabig114222/departments.avsc
hdfs dfs -rm -r /user/anabig114222/titles.avsc
hdfs dfs -rm -r /user/anabig114222/salaries.avsc


#/*displaying all the database in mysql server*/

sqoop list-databases --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306 --username anabig114222 --password Bigdata123
 
#/*dislpaying all the tables in mysql databases */

sqoop list-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114222 --username anabig114222 --password Bigdata123

#/*import all the files in avrodatafile format */

sqoop import-all-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114222 --username anabig114222 --password Bigdata123  --compression-codec=snappy --as-avrodatafile --warehouse-dir=/user/anabig114222/capstoneproject/warehouse/ --m 1  --driver com.mysql.jdbc.Driver   

#/*importing data in parquet format the */
sqoop import-all-tables --connect jdbc:mysql://ip-10-1-1-204.ap-south-1.compute.internal:3306/anabig114222 --username anabig114222 --password Bigdata123  --compression-codec=snappy --as-parquetfile --warehouse-dir=/user/anabig114222/capstoneproject/warehouse2/ --m 1  --driver com.mysql.jdbc.Driver     

#.avsc files put into the user dir
hdfs dfs -put /home/anabig114222/departments.avsc /user/anabig114222/
hdfs dfs -put /home/anabig114222/employees.avsc /user/anabig114222/
hdfs dfs -put /home/anabig114222/titles.avsc /user/anabig114222/
hdfs dfs -put /home/anabig114222/salaries.avsc /user/anabig114222/
hdfs dfs -put /home/anabig114222/dept_emp.avsc /user/anabig114222/
hdfs dfs -put /home/anabig114222/dept_manager.avsc /user/anabig114222/