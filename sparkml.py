#!/usr/bin/env python
# coding: utf-8

# In[1]:


### Initialize the spark
#from pyspark.sql import SparkSession
#spark = SparkSession.builder.getOrCreate()


# In[2]:

#initlize the spark
pyspark


# In[3]:

#loading the parquet files hdfs to spark 
department_sql = spark.read.parquet("hdfs:///user/anabig114222/capstoneproject/warehouse2/departments/d2687b87-f8db-4a39-9083-2f4d9876a1e2.parquet")


# In[4]:


department_sql.show(5)


# In[5]:


dept_emp_sql = spark.read.parquet("hdfs:///user/anabig114222/capstoneproject/warehouse2/dept_emp/b816a697-b952-43bd-b52e-04e388fc400f.parquet")


# In[6]:


dept_emp_sql.show(5)


# In[7]:


dept_manager_sql = spark.read.parquet("hdfs:///user/anabig114222/capstoneproject/warehouse2/dept_manager/c4b1488a-7059-4cdc-982b-7c8cce57cb2a.parquet")


# In[8]:


dept_manager_sql.show(5)


# In[9]:


employees_sql = spark.read.parquet("hdfs:///user/anabig114222/capstoneproject/warehouse2/employees/5e761c6f-e541-45d9-b628-3982d952da8e.parquet")


# In[10]:


employees_sql.show(5)


# In[11]:


salaries_sql = spark.read.parquet("hdfs:///user/anabig114222/capstoneproject/warehouse2/salaries/3ea6d465-0b59-4a4e-a359-a83aac7f00b1.parquet")


# In[12]:


salaries_sql.show()


# In[13]:


titles_sql= spark.read.parquet("hdfs:///user/anabig114222/capstoneproject/warehouse2/titles/c822d250-d438-45f0-adac-5fb8fef7880b.parquet")


# In[14]:


titles_sql.show()


# In[15]:


#create a sql tempview to run sql commands
titles_sql.createTempView('titles_temp')


# In[16]:


spark.sql('select * from titles_temp limit 5').show()


# In[17]:


salaries_sql.createTempView('salaries_temp')


# In[18]:


spark.sql('select * from salaries_temp limit 5').show()


# In[19]:


employees_sql.createTempView('employees_temp')


# In[20]:


spark.sql('select * from employees_temp limit 5').show()


# In[21]:


dept_manager_sql.createTempView('dept_manager_temp')


# In[22]:


spark.sql('select * from dept_manager_temp limit 5').show()


# In[23]:


department_sql.createTempView('department_temp')


# In[24]:


spark.sql('select * from department_t  emp limit 5').show()


# In[25]:


dept_emp_sql.createTempView('dept_emp_temp')


# In[26]:


spark.sql('select * from dept_emp_temp limit 5 ').show()


# In[27]:

#Analycs the data 

#A list showing employee number, last name, first name, sex, and salary for each employee 

spark.sql('select e.emp_no,first_name,last_name, sex,salary  from  employees_temp e INNER JOIN salaries_temp s on e.emp_no=s.emp_no').show()


# In[28]:


#A list showing first name, last name, and hire date for employees who were hired in 1986.**/

spark.sql("select first_name,last_name,hire_date, year(date_format(from_unixtime(unix_timestamp(cast(hire_date as string),'mm/dd/yyyy')),'yyyy-MM-dd')) AS year_ FROM employees_temp where year(date_format(from_unixtime(unix_timestamp(cast(hire_date as string),'mm/dd/yyyy')),'yyyy-MM-dd'))=1986").show()


# In[29]:


#A list showing the manager of each department with the following information: department number, department name,the manager's employee number, last name, first name.
spark.sql('SELECT dm.dept_no ,e.emp_no,e.last_name,e.first_name,dp.dept_name FROM department_temp dp INNER JOIN dept_manager_temp dm on dp.dept_no=dm.dept_no INNER JOIN employees_temp e on dm.emp_no=e.emp_no').show()


# In[30]:


# A list showing the department of each employee with the following information: employee number, last name, first  name, and department name.*/
spark.sql('SELECT e.emp_no,e.last_name,e.first_name,dp.dept_name  FROM department_temp dp inner join dept_emp_temp de on dp.dept_no=de.dept_no inner join employees_temp e on de.emp_no=e.emp_no').show()


# In[172]:


# A list showing first name, last name, and sex for employees whose first name is "Hercules" and last names begin with "B.“ */

spark.sql("SELECT last_name,first_name,sex FROM employees_temp WHERE first_name= 'Hercules' and last_name like 'B%'").show()


# In[178]:


#5. A list showing all employees in the Sales department, including their employee number, last name, first name, and  department name. */
spark.sql("select e.emp_no,e.last_name,e.first_name, dp.dept_name from  department_temp dp inner join  dept_emp_temp de on dp.dept_no = de.dept_no inner join employees_temp e on de.emp_no = e.emp_no where dept_name like '%Sales%'").show()


# In[181]:


# A list showing all employees in the Sales and Development departments, including their employee number, last name,  first name, and department name. **/
spark.sql("select e.emp_no,e.last_name,e.first_name, dp.dept_name from  department_temp dp inner join  dept_emp_temp de on dp.dept_no = de.dept_no inner join employees_temp e on de.emp_no = e.emp_no where dept_name like '%Sales%' OR dept_name like '%development%'").show()


# In[184]:


#A list showing the frequency count of employee last names, in descending order. ( i.e., how many employees share each  last name */
spark.sql('SELECT first_name,last_name, count(last_name) as fre_count FROM employees_temp  GROUP BY first_name,last_name ORDER BY fre_count desc').show()


# In[35]:


#total salary pay to the company to the empoyees
spark.sql('select sum(salary) as total from salaries_temp').show()


# In[36]:
import pandas
import matplotlib.pyplot as plt
import seaborn as sns

sns.distplot(spark.sql("select emp_no , sum(salary) from salaries_temp group by emp_no ").toPandas(), norm_hist= True)



# show the Average salary per title (designation) */
spark.sql('select t.title,avg(s.salary) as avg_salary from titles_temp t inner join employees_temp e on t.title_id=e.emp_title_id inner join salaries_temp s on e.emp_no=s.emp_no group by t.title').show()

#barplot show the Average salary per title
sns.barplot(x='title' , y='avg(salary)', data = spark.sql("select t.title, avg(s.salary) from employees_temp e inner join titles_temp t on e.emp_title_id = t.title_id inner join salaries s on e.emp_no = s.emp_no group by t.title").toPandas() )
plt.show()

# In[37]:


#. the total salary per title (designation) */
spark.sql('select t.title,sum(s.salary) as total_salary from titles_temp t inner join employees_temp e on t.title_id=e.emp_title_id  inner join salaries_temp s on e.emp_no=s.emp_no group by t.title').show()


# In[38]:


#the Average salary per dept_name (designation) */
spark.sql('SELECT d.dept_name,avg(s.salary) avg_salary FROM department_temp d INNER JOIN dept_emp_temp de on d.dept_no=de.dept_no INNER JOIN employees_temp e on de.emp_no=e.emp_no INNER JOIN salaries_temp s on e.emp_no=s.emp_no GROUP BY d.dept_name').show()


# In[39]:


#the total salary per dept_name (designation) */
spark.sql('SELECT d.dept_name,sum(s.salary) total_salary FROM department_temp d INNER JOIN dept_emp_temp de on d.dept_no=de.dept_no INNER JOIN employees_temp e on de.emp_no=e.emp_no INNER JOIN salaries_temp s on e.emp_no=s.emp_no GROUP BY d.dept_name').show()


# In[40]:


#the Average salary per sex (designation) */
spark.sql('SELECT sex,avg(salary) FROM employees_temp e INNER JOIN salaries_temp s on e.emp_no=s.emp_no GROUP BY sex').show()


# # using Joins funcation to join the tables

# In[41]:


title_join=spark.sql('select emp_no,emp_title_id,title from employees_temp e left join titles_temp t on e.emp_title_id=t.title_id')


# In[42]:


salary_join=spark.sql('select e.emp_no,salary from employees_temp e left join salaries_temp s on e.emp_no=s.emp_no')


# In[43]:


title_join.createTempView('title_join1')


# In[44]:


salary_join.createTempView('salary_join1')


# In[45]:


spark.sql('select * from title_join1').show()


# In[46]:


spark.sql('select * from salary_join1').show()


# In[61]:


emp_join=spark.sql('select t.emp_no,emp_title_id,title,salary from title_join1 t inner join salary_join1 s on t.emp_no=s.emp_no')


# In[58]:


department_join=spark.sql('select e.emp_no,dp.dept_no,emp_title_id,sex,no_of_projects,left_company,dept_name,Last_performance_rating from employees_temp e inner join dept_emp_temp de on e.emp_no=de.emp_no inner join department_temp dp on de.dept_no=dp.dept_no ')


# In[64]:


emp_join.createTempView('emp_joins')


# In[50]:


department_join.createTempView('department_join1')


# In[65]:


spark.sql('select * from emp_joins').show()


# In[52]:


spark.sql('select * from department_join1').show()


# In[89]:


final_data=spark.sql('select e.emp_no,title,salary,sex,no_of_projects,dept_name,Last_performance_rating,left_company from emp_joins e left join department_join1 d on e.emp_no=d.emp_no')


# In[98]:


final_data.createTempView('final_datasets')


# In[92]:


final_data.show()


# In[93]:

#importing the pyspark funcation
from pyspark.sql.functions import col,count,isnan,when


# In[94]:


#checking for null values in our columns       
final_data.select([count(when(col(c).isNull(),c)).alias(c) for c in final_data.columns]).show()  


# In[95]:

#distinct titles
final_data.select('title').distinct().show()


# In[96]:

#distinct dept_name
final_data.select('dept_name').distinct().show()



#final_datasets


# In[99]:


spark.sql('select dept_name,count(dept_name) as total from final_datasets group by dept_name').show()


# In[100]:


spark.sql('select title,count(title) as total from final_datasets group by title').show()


# In[101]:


spark.sql('select sex,count(sex) as total from final_datasets group by sex').show()


# In[102]:


spark.sql('select Last_performance_rating,count(Last_performance_rating) as total from final_datasets group by Last_performance_rating').show()


# In[103]:


spark.sql('select left_company,count(left_company) as total from final_datasets group by left_company').show()


# In[104]:


final_data.printSchema()


# In[117]:


#import the required libraries
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import VectorAssembler


# In[113]:


##  numeric indexing for the strings (indexing starts from 0)
indexer = StringIndexer(inputCol="sex", outputCol="sexNumericIndex")


# In[114]:


## fit the indexer model and use it to transform the strings into numeric indices
df = indexer.fit(final_data).transform(final_data)


# In[119]:


df.show()


# In[ ]:


#StringIndexer converting the string to numeric formate


# In[120]:


indexer = StringIndexer(inputCol="title", outputCol="titleNumericIndex")


# In[121]:


df1 = indexer.fit(df).transform(df)


# In[122]:


indexer = StringIndexer(inputCol="Last_performance_rating", outputCol="Last_performance_ratingNumericIndex")


# In[123]:


df2 = indexer.fit(df1).transform(df1)


# In[124]:


indexer = StringIndexer(inputCol="dept_name", outputCol="dept_nameNumericIndex")


# In[125]:


df3 = indexer.fit(df2).transform(df2)


# In[126]:


df3.show()


# In[127]:


df3.printSchema()


# In[186]:


#VectorAssembler


# In[128]:


assesmble=VectorAssembler(inputCols=['sexNumericIndex','titleNumericIndex','Last_performance_ratingNumericIndex','dept_nameNumericIndex','no_of_projects','salary'],outputCol='features')


# In[129]:


#the transform method to  transform our data 
df4=assesmble.transform(df3)


# In[130]:


df4.show()


# In[131]:


df5=df4.select('features','left_company')


# In[132]:


df5.show()


# In[133]:


#spliting the data train ,test
(train, test) = df5.randomSplit([.7,.3])


# In[134]:


#train
train.show()


# In[135]:


#test
test.show()


# In[136]:


#import the required libraries
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LogisticRegression


# In[167]:


#RandomForestClassifier


# In[137]:


rf = RandomForestClassifier(labelCol='left_company', 
                            featuresCol='features',
                            maxDepth=5)


# In[138]:


model = rf.fit(train)


# In[139]:


rf_predictions = model.transform(test)


# In[140]:


from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# In[141]:


multi_evaluator = MulticlassClassificationEvaluator(labelCol = 'left_company', metricName = 'accuracy')


# In[142]:


print('Random Forest classifier Accuracy:', multi_evaluator.evaluate(rf_predictions))


# In[165]:


#LogisticRegression


# In[154]:


lr = LogisticRegression(featuresCol ='features', labelCol ='left_company', maxIter=10)


# In[155]:


lrModel = lr.fit(train)


# In[156]:


lr_predictions = lrModel.transform(test)


# In[157]:


log_multi_evaluator = MulticlassClassificationEvaluator(labelCol = 'left_company', metricName = 'accuracy')


# In[158]:


print('LogisticRegression Accuracy:', log_multi_evaluator.evaluate(lr_predictions))


# In[166]:


#DecisionTreeClassifier


# In[159]:


from pyspark.ml.classification import DecisionTreeClassifier


# In[160]:


dt = DecisionTreeClassifier(featuresCol = 'features', labelCol = 'left_company', maxDepth = 3)


# In[161]:


dtModel = dt.fit(train)


# In[162]:


dt_predictions = dtModel.transform(test)


# In[185]:


#MulticlassClassificationEvaluator


# In[163]:


multi_evaluator = MulticlassClassificationEvaluator(labelCol = 'left_company', metricName = 'accuracy')


# In[164]:


print('Decision Tree Accuracy:', multi_evaluator.evaluate(dt_predictions))


# In[ ]:


spark.stop()      

