import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import time
import logging
import pytest

# Setting logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

time.sleep(30)
logging.info('Hello, Connecting!')

# Read DB connection info from environment variables
db_user = os.getenv('POSTGRES_USER')
db_pass = os.getenv('POSTGRES_PASSWORD')
db_name = os.getenv('POSTGRES_DB')

# Create the SQLAlchemy engine
# pg = create_engine(f'postgresql://{db_user}:{db_pass}@postgresdb/{db_name}', echo=False)

def get_engine():
    engine=  create_engine(
        f'postgresql://{db_user}:{db_pass}@postgresdb/{db_name}',
        echo=False)
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logging.info("✅ Successfully connected to PostgreSQL.")
    except Exception as e:
        logging.error("❌ Failed to connect to PostgreSQL.")
        logging.error(e)
        raise

    return engine

pg = get_engine()

time.sleep(5)

def run_query(engine, query, title=None):
    """Run a SQL query and print the results.

    Args:
        query (str): SQL query to execute.
        title (str, optional): Optional title to display above results."""
    
    if title:
        print(f"\n\n\n-------* {title} *-------")
    else:
        print("\n\n\n-------* Query Result *-------")
    
    try:
        df = pd.read_sql(query, engine)
        print(df)
        #return df
    except Exception as e:
        logging.error("Error executing query:")
        logging.error(e)
        raise


run_query(pg, """select * from regions;""", 'check test')


time.sleep(5)
# Queries
print('\n\n\n---*Top 5 highest paid employees*---')
query_01 = """select * from employees
               where salary > 100000
               order by salary desc
               limit 5;"""

df_01 = pd.read_sql(query_01, pg)
print(df_01)


print('\n\n\n---*Total salary broken by department and gender*---')
query_02 = """select department, gender, sum(salary)
from employees
group by department, gender
order by sum(salary) desc;"""

df_02 = pd.read_sql(query_02,pg)
print(df_02)


print('\n\n\n---*How many employees without an email?*---')
query_03 = """select count(*)
from employees
where email is NULL;"""

df_03 = pd.read_sql(query_03, pg)
print(df_03)


print('\n\n\n---*How many under paid, paid well, executive?*---')
query_04 = """select a.category, count(*)
from (
select first_name||' '||last_name full_name, salary,
case
    when salary < 100000 then 'UNDER PAID'
	when salary > 100000 and salary < 160000 then 'PAID WELL'
	when salary > 160000 then 'EXECUTIVE'
	else 'UNPAID'
end as category
from employees
order by salary desc )a
group by a.category
order by count(*) desc;"""

df_04 = pd.read_sql(query_04, pg)
print(df_04)


print('\n\n\n---*Transposing the previous query*---')
query_05 = """select sum ( case when salary < 100000 then 1 else 0  end) as UNDER_PAID,
sum ( case when salary > 100000 and salary < 160000 then 1 else 0 end) as PAID_WELL,
sum( case when salary > 160000 then 1 else 0 end) as EXECUTIVE
from employees;"""

df_05 = pd.read_sql(query_05, pg)
print(df_05)


print('\n\n\n')
query_06 = """select first_name||' '||last_name full_name, case when region_id = 1 then 'USA' end as region_1_SW_USA,
case when region_id = 2 then 'USA' end as region_2_NE_USA,
case when region_id = 3 then 'USA' end as region_3_NW_USA,
case when region_id = 4 then 'ASIA' end as region_4_CENTRAL_ASIA,
case when region_id = 5 then 'ASIA' end as region_5_EAST_ASIA,
case when region_id = 6 then 'CANADA' end as region_6_QUEBEC_CANADA,
case when region_id = 7 then 'CANADA' end as region_7_NOVA_SCOTIA_CANADA
from employees;"""

df_06 = pd.read_sql(query_06, pg)
print(df_06)


print('\n\n\n')
query_07 = """select first_name||' '||last_name full_name, country
from  employees inner join regions
on employees.region_id = regions.region_id;"""

df_07 = pd.read_sql(query_07, pg)
print(df_07)


print('\n\n\n')
query_08 = """select first_name||' '||last_name, email, division, country
from employees inner join departments
on employees.department = departments.department
inner join regions on employees.region_id = regions.region_id --join w/ the result of the others
where email is not null;"""

df_08 = pd.read_sql(query_08, pg)
print(df_08)


print('\n\n\n')
query_09 = """select first_name||' '||last_name full_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id
where hire_date = (select max(hire_date) from (select first_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id)a)
union all
(select first_name||' '||last_name full_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id
where hire_date = (select min(hire_date) from (select first_name, last_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id) b)
limit 1)
order by hire_date;"""

df_09 = pd.read_sql(query_09, pg)
print(df_09)


print('\n\n\n')
query_10 = """select first_name||' '||last_name full_name, department, count(*) over(partition by department)
from employees;"""

df_10 = pd.read_sql(query_10, pg)
print(df_10)


print('\n\n\n')
query_11 = """select department, count(*) as total_employees, sum(salary) as total_salary
from employees
group by department
union all
select 'TOTAL', count(*), sum(salary)
from employees;"""

df_11 = pd.read_sql(query_11, pg)
print(df_11)


print('\n\n\n---*The total salary paid by each department?*---')
query_12 = """select department, sum(salary) as total_salary
              from employees
              group by department
              order by total_salary desc;"""

df_12 = pd.read_sql(query_12, pg)
print(df_12)


print('\n\n\n')
query_13 = """select first_name||' '||last_name full_name, department, sum(salary) over(partition by department) total_salary
              from employees
              order by total_salary desc;"""

df_13 = pd.read_sql(query_13, pg)
print(df_13)


print('\n\n\n---*how many under paid, paid well or executiv are there?*---')
query_14 = """select a.category, count(*) total_employees, sum(a.salary) total_salary
              from (
              select first_name, salary,
              case
                  when salary < 100000 then 'UNDER PAID'
	              when salary > 100000 and salary < 160000 then 'PAID WELL'
	              when salary > 160000 then 'EXECUTIVE'
	              else 'UNPAID'
              end as category
              from employees
              order by salary desc )a
              group by a.category
              order by count(*) desc;"""

df_14 = pd.read_sql(query_14, pg)
print(df_14)


print('\n\n\n---*rank employees table over department based on salary highest to low*---')
query_15 = """select first_name, last_name, department, salary,
              rank() over(partition by department order by salary desc)
              from employees;"""

df_15 = pd.read_sql(query_15, pg)
print(df_15)


print('\n\n\n---*All the highest paid employees for each department*---')
query_16 = """select a.first_name||' '||a.last_name full_name, a.department, a.salary
              from (select first_name, last_name, department, salary,
              rank() over(partition by department order by salary desc)
              from employees) a
              where rank = 1
              order by salary desc;"""

df_16 = pd.read_sql(query_16, pg)
print(df_16)


print('\n\n\n---*creat a two column with lead and lag*---')
query_17 = """select first_name||' '||last_name full_name, department,
              rank() over(partition by department order by salary desc) as salary_rank,
              salary,
              lead(salary) over() as next_salary,
              lag(salary) over() as previous_salary
              from employees;"""

df_17 = pd.read_sql(query_17, pg)
print(df_17)


print('\n\n\n')
query_18 = """select first_name||' '||last_name full_name, department, salary,
              lead(salary) over(order by salary desc) as closest_lower_employee,
              lag(salary) over(order by salary desc) as closest_higher_employee
              from employees;"""

df_18 = pd.read_sql(query_18, pg)
print(df_18)


print('\n\n\n')
query_19 = """select first_name||' '||last_name full_name, department, salary,
              lead(salary) over(partition by department order by salary desc) as closest_lower_employee,
              lag(salary) over(partition by department order by salary desc) as closest_higher_employee
              from employees;"""

df_19 = pd.read_sql(query_19, pg)
print(df_19)


print('\n\n\n---*how many employees with the same first name?*---')
query_20 = """select first_name, count(*) count_name
              from employees
              group by first_name
              having count(*) != 1
              order by count(*) desc;"""

df_20 = pd.read_sql(query_20, pg)
print(df_20)


print('\n\n\n---*let''s transpose that information*---')
query_21 = """select sum(case when a.count_name = 3 then 1 else 0 end) count_3,
              sum(case when a.count_name = 2 then 1 else 0 end) count_2
              from (select first_name, count(*) count_name
              from employees
              group by first_name
              having count(*) !=1
              order by count(*) desc)a;"""

df_21 = pd.read_sql(query_21, pg)
print(df_21)


print("\n\n\n---*how many employees in Sports, Tools, Clothing, Computers department? show in a transposed way*---")
query_22 = """select sum( case when department = 'Sports' then 1 else 0 end ) as Sports,
              sum( case when department = 'Tools' then 1 else 0 end ) as Tools,
              sum( case when department = 'Clothing' then 1 else 0 end ) as Clothing,
              sum( case when department = 'Computers' then 1 else 0 end ) as Computers
              from employees;"""

df_22 = pd.read_sql(query_22, pg)
print(df_22)


print('\n\n\n---*how many employees with the same email domain?*---')
query_23 = """select a.email_domain, count(*)
              from (select substring(email, position('@' in email)+1) as email_domain
              from employees)a
              where a.email_domain is not null
              group by a.email_domain
              order by count(*) desc;"""

df_23 = pd.read_sql(query_23, pg)
print(df_23)


print('\n\n\n---*min, max, avg salary broken down by gender, region, country*---')
query_24 = """select e.gender gender, e.region_id id_region, r.region||r.country country,min(salary) min, max(salary) max, round(avg(salary)) avg
              from employees e inner join regions r on e.region_id = r.region_id
              group by gender, id_region, r.region||r.country
              order by gender;"""

df_24 = pd.read_sql(query_24, pg)
print(df_24)


print('\n\n\n-----CTE for Department String Replacement and Concatenated Output-----')
query_25 = """with modified_clothing_department as ( select department,
                                         replace(department, 'Clothing', 'ATTIRE') modified_dept
										 from employees)
select department,
       modified_dept,
	   concat(modified_dept, '_', 'department') complete_mod_dept
from modified_clothing_department;"""

df_25 = pd.read_sql(query_25, pg)
print(df_25)


print('\n\n\n-----Lists employees with department-level salary sums and each salary’s difference from the department average.-----')
query_26 = """select concat(first_name, ' ', last_name) full_name, 
       department, 
	   sum(salary) over(partition by department) total_dept_salary, 
	   salary,
	   (select round(avg(salary)) from employees where e1.department = department) avg_dept_sal,
      (salary - (select round(avg(salary)) from employees where e1.department = department)) gap_dept_sal
from employees e1
order by department, gap_dept_sal;"""

df_26 = pd.read_sql(query_26, pg)
print(df_26)


print('\n\n\n----Which are the lowest and highest paid employees per department?---')
query_27 = """with ranked_employees as (
		select first_name||' '||last_name full_name,
		department,
		salary,
		rank() over (partition by department order by salary desc) as max_rnk,
		rank() over (partition by department order by salary asc) as min_rnk
		from employees
)
select full_name, department, salary,
       case when max_rnk = 1 then 'HIGHEST DEPT SAL'
		     when min_rnk = 1 then 'LOWEST DEPT SAL'
			 end salary_status
from ranked_employees
where max_rnk = 1 or min_rnk = 1
order by department;
"""

df_27 = pd.read_sql(query_27, pg)
print(df_27)


print("\n\n\n-----What's the hire year range?-----")
query_28 = """select extract (year from min(hire_date)) earliest_hire_year, extract (year from max(hire_date)) latest_hire_year
from employees;"""

df_28 = pd.read_sql(query_28, pg)
print(df_28)


print("\n\n\n-----What's the total hire over the years?-----")
query_29 = """select extract(year from hire_date) hiring_year, count(*) as yearly_total
from employees
group by (extract (year from hire_date))
order by hiring_year;"""

df_29 = pd.read_sql(query_29, pg)
print(df_29)


print("\n\n\n----What's the total hire over the years break down by department?-----")
query_30 = """select extract(year from hire_date) hiring_year, department, count(*) as yearly_total
from employees
group by (extract (year from hire_date)), department
order by hiring_year;"""

df_30 = pd.read_sql(query_30, pg)
print(df_30)


print("\n\n\n-----Total amount of hired by department?-----")
query_31 = """with hiring_dept as (select extract(year from hire_date) hiring_year, department, count(*) as yearly_total
from employees
group by (extract (year from hire_date)), department
order by hiring_year)
select department, sum(yearly_total) total
from hiring_dept
group by department
order by total desc;"""

df_31 = pd.read_sql(query_31, pg)
print(df_31)


print("\n\n\n-----Salary distributions into 10 buckets-----")
query_32 = """with buckets as (
select width_bucket(salary, 20000, 170000, 10) as bucket, count(*) total_empl
from employees
group by bucket
order by bucket)
select bucket, 
        case 
		    when bucket = 0 then 'Below 20000'
			when bucket = 11 then 'Above 170000'
			else concat(20000 + (bucket -1) * 15000, ' --> ', 20000 + bucket*15000) 
			end as salary_range, 
			total_empl
from buckets
order by bucket;"""

df_32 = pd.read_sql(query_32, pg)
print(df_32)


print("\n\n\n-----Total number of employees hired over the years, shown by quarter.-----")
query_33 = """select 
     case 
	      when extract (month from hire_date) in (1,2,3,4) then 'First_Quarter:Jan-Apr'
	      when extract (month from hire_date) in (5,6,7,8) then 'Second Quarter: May-Aug'
	      when extract (month from hire_date) in (9,10,11,12) then 'Third Quarter:Sept-Dec'
	end quarters,
	count(*) total_hire
from employees 
group by quarters
order by quarters;"""

df_33 = pd.read_sql(query_33, pg)
print(df_33)


print("\n\n\n-----Female/Male median salary ratio.-----")
query_34 = """WITH med AS (
    SELECT
        gender,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) AS med_salary
    FROM employees
    GROUP BY gender
)
SELECT
    (SELECT med_salary FROM med WHERE gender = 'F') /
    (SELECT med_salary FROM med WHERE gender = 'M') AS gender_pay_ratio;"""

df_34 = pd.read_sql(query_34, pg)
print(df_34)


print("\n\n\n-----Salary distributions (F/M) into 10 buckets-----")
query_35 = """WITH buckets AS (
    SELECT
        width_bucket(salary, 20000, 170000, 10) AS bucket,
        gender,
        count(*) AS total_f_m_empl
    FROM employees
    GROUP BY bucket, gender
),
ranges AS (
    SELECT
        bucket,
        gender,
        total_f_m_empl,
        CASE 
            WHEN bucket = 0 THEN 'Below 20000'
            WHEN bucket = 11 THEN 'Above 170000'
            ELSE CONCAT(20000 + (bucket - 1) * 15000, ' --> ', 20000 + bucket * 15000)
        END AS salary_range
    FROM buckets
)
SELECT 
    r.*,
    SUM(r.total_f_m_empl) OVER (PARTITION BY r.bucket) AS total_in_range
FROM ranges r
ORDER BY r.bucket;"""

df_35 = pd.read_sql(query_35, pg)
print(df_35)

logging.info('---*Congratulations, all the queries are running correctly*---')


