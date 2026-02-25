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

def run_query(engine, id, query, title=None):
    """Run a SQL query and print the results.

    Args:
        id(int): query number for display
        query (str): SQL query to execute.
        title (str, optional): Optional title to display above results."""
    
    if title:
        print(f"\n\n\n-------* {id}. {title} *-------")
    else:
        print(f"\n\n\n-------* {id}. Query Result *-------")
    
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        logging.error("Error executing query:")
        logging.error(e)
        raise


df_00 = run_query(pg, 0, """select * from regions;""", 'check test')
print(df_00)

time.sleep(5)


# Queries
df_01 = run_query(pg, 1, """select * from employees
               where salary > 100000
               order by salary desc
               limit 5;""", 'Top 5 highest paid employees')
print(df_01)

df_02 = run_query(pg, 2, """select department, gender, sum(salary)
from employees
group by department, gender
order by sum(salary) desc;""", 'Total salary broken by department and gender')
print(df_02)

df_03 = run_query(pg, 3, """select count(*)
from employees
where email is NULL;""", 'How many employees without an email?')
print(df_03)

df_04 = run_query(pg, 4,  """select a.category, count(*)
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
order by count(*) desc;""",'How many under paid, paid well, executive?')
print(df_04)

df_05 = run_query(pg, 5, """select sum ( case when salary < 100000 then 1 else 0  end) as UNDER_PAID,
sum ( case when salary > 100000 and salary < 160000 then 1 else 0 end) as PAID_WELL,
sum( case when salary > 160000 then 1 else 0 end) as EXECUTIVE
from employees;""", 'Transposing the previous query')
print(df_05)

df_06 = run_query(pg, 6, """select first_name||' '||last_name full_name, case when region_id = 1 then 'USA' end as region_1_SW_USA,
case when region_id = 2 then 'USA' end as region_2_NE_USA,
case when region_id = 3 then 'USA' end as region_3_NW_USA,
case when region_id = 4 then 'ASIA' end as region_4_CENTRAL_ASIA,
case when region_id = 5 then 'ASIA' end as region_5_EAST_ASIA,
case when region_id = 6 then 'CANADA' end as region_6_QUEBEC_CANADA,
case when region_id = 7 then 'CANADA' end as region_7_NOVA_SCOTIA_CANADA
from employees;""")
print(df_06)

df_07 = run_query(pg, 7, """select first_name||' '||last_name full_name, country
from  employees inner join regions
on employees.region_id = regions.region_id;""")
print(df_07)

df_08 = run_query(pg, 8, """select first_name||' '||last_name, email, division, country
from employees inner join departments
on employees.department = departments.department
inner join regions on employees.region_id = regions.region_id --join w/ the result of the others
where email is not null;""", )
print(df_08)

df_09 = run_query(pg, 9, """select first_name||' '||last_name full_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id
where hire_date = (select max(hire_date) from (select first_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id)a)
union all
(select first_name||' '||last_name full_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id
where hire_date = (select min(hire_date) from (select first_name, last_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id) b)
limit 1)
order by hire_date;""")
print(df_09)

df_10 = run_query(pg, 10, """select first_name||' '||last_name full_name, department, count(*) over(partition by department)
from employees;""")
print(df_10)

df_11 = run_query(pg, 11, """select department, count(*) as total_employees, sum(salary) as total_salary
from employees
group by department
union all
select 'TOTAL', count(*), sum(salary)
from employees;""")
print(df_11)


df_12 = run_query(pg, 12, """select department, sum(salary) as total_salary
              from employees
              group by department
              order by total_salary desc;""", 'The total salary paid by each department?')
print(df_12)

df_13 = run_query(pg, 13, """select first_name||' '||last_name full_name, department, sum(salary) over(partition by department) total_salary
              from employees
              order by total_salary desc;""")
print(df_13)

df_14 = run_query(pg, 14, """select a.category, count(*) total_employees, sum(a.salary) total_salary
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
              order by count(*) desc;""", 'how many under paid, paid well or executiv are there?')
print(df_14)

df_15 = run_query(pg, 15, """select first_name, last_name, department, salary,
              rank() over(partition by department order by salary desc)
              from employees;""", 'rank employees table over department based on salary highest to low')
print(df_15)

df_16 = run_query(pg, 16, """select a.first_name||' '||a.last_name full_name, a.department, a.salary
              from (select first_name, last_name, department, salary,
              rank() over(partition by department order by salary desc)
              from employees) a
              where rank = 1
              order by salary desc;""", 'All the highest paid employees for each department')
print(df_16)

df_17 = run_query(pg, 17, """select first_name||' '||last_name full_name, department,
              rank() over(partition by department order by salary desc) as salary_rank,
              salary,
              lead(salary) over() as next_salary,
              lag(salary) over() as previous_salary
              from employees;""", 'create a two column with lead and lag')
print(df_17)

df_18 = run_query(pg, 18, """select first_name||' '||last_name full_name, department, salary,
              lead(salary) over(order by salary desc) as closest_lower_employee,
              lag(salary) over(order by salary desc) as closest_higher_employee
              from employees;""")
print(df_18)

df_19 = run_query(pg, 19, """select first_name||' '||last_name full_name, department, salary,
              lead(salary) over(partition by department order by salary desc) as closest_lower_employee,
              lag(salary) over(partition by department order by salary desc) as closest_higher_employee
              from employees;""")
print(df_19)

df_20 = run_query(pg, 20, """select first_name, count(*) count_name
              from employees
              group by first_name
              having count(*) != 1
              order by count(*) desc;""", 'how many employees with the same first name?')
print(df_20)

df_21 = run_query(pg, 21, """select sum(case when a.count_name = 3 then 1 else 0 end) count_3,
              sum(case when a.count_name = 2 then 1 else 0 end) count_2
              from (select first_name, count(*) count_name
              from employees
              group by first_name
              having count(*) !=1
              order by count(*) desc)a;""", 'let''s transpose that information')
print(df_21)

df_22 = run_query(pg, 22, """select sum( case when department = 'Sports' then 1 else 0 end ) as Sports,
              sum( case when department = 'Tools' then 1 else 0 end ) as Tools,
              sum( case when department = 'Clothing' then 1 else 0 end ) as Clothing,
              sum( case when department = 'Computers' then 1 else 0 end ) as Computers
              from employees;""", 'how many employees in Sports, Tools, Clothing, Computers department? show in a transposed way')
print(df_22)

df_23 = run_query(pg, 23, """select a.email_domain, count(*)
              from (select substring(email, position('@' in email)+1) as email_domain
              from employees)a
              where a.email_domain is not null
              group by a.email_domain
              order by count(*) desc;""", 'how many employees with the same email domain?')
print(df_23)

df_24 = run_query(pg, 24, """select e.gender gender, e.region_id id_region, r.region||r.country country,min(salary) min, max(salary) max, round(avg(salary)) avg
              from employees e inner join regions r on e.region_id = r.region_id
              group by gender, id_region, r.region||r.country
              order by gender;""", 'min, max, avg salary broken down by gender, region, countr')
print(df_24)

df_25 = run_query(pg, 25, """with modified_clothing_department as ( select department,
                                         replace(department, 'Clothing', 'ATTIRE') modified_dept
										 from employees)
select department,
       modified_dept,
	   concat(modified_dept, '_', 'department') complete_mod_dept
from modified_clothing_department;""", 'CTE for Department String Replacement and Concatenated Output')
print(df_25)

df_26 = run_query(pg, 26, """select concat(first_name, ' ', last_name) full_name, 
       department, 
	   sum(salary) over(partition by department) total_dept_salary, 
	   salary,
	   (select round(avg(salary)) from employees where e1.department = department) avg_dept_sal,
      (salary - (select round(avg(salary)) from employees where e1.department = department)) gap_dept_sal
from employees e1
order by department, gap_dept_sal;""", "Lists employees with department-level salary sums and each salary difference from the department average.")
print(df_26)

df_27 = run_query(pg, 27, """with ranked_employees as (
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
""", 'Which are the lowest and highest paid employees per department?')
print(df_27)

df_28 = run_query(pg, 28, """select extract (year from min(hire_date)) earliest_hire_year, extract (year from max(hire_date)) latest_hire_year
from employees;""", "What's the hire year range?")
print(df_28)

df_29 = run_query(pg, 29,  """select extract(year from hire_date) hiring_year, count(*) as yearly_total
from employees
group by (extract (year from hire_date))
order by hiring_year;""", "What's the total hire over the years?")
print(df_29)

df_30 = run_query(pg, 30, """select extract(year from hire_date) hiring_year, department, count(*) as yearly_total
from employees
group by (extract (year from hire_date)), department
order by hiring_year;""", "What's the total hire over the years break down by department?")
print(df_30)

df_31 = run_query(pg, 31, """with hiring_dept as (select extract(year from hire_date) hiring_year, department, count(*) as yearly_total
from employees
group by (extract (year from hire_date)), department
order by hiring_year)
select department, sum(yearly_total) total
from hiring_dept
group by department
order by total desc;""", "Total amount of hired by department?")
print(df_31)

df_32 = run_query(pg, 32, """with buckets as (
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
order by bucket;""", "Salary distributions into 10 buckets")
print(df_32)

df_33 = run_query(pg, 33, """select 
     case 
	      when extract (month from hire_date) in (1,2,3,4) then 'First_Quarter:Jan-Apr'
	      when extract (month from hire_date) in (5,6,7,8) then 'Second Quarter: May-Aug'
	      when extract (month from hire_date) in (9,10,11,12) then 'Third Quarter:Sept-Dec'
	end quarters,
	count(*) total_hire
from employees 
group by quarters
order by quarters;""", "Total number of employees hired over the years, shown by quarter.")
print(df_33)

df_34 = run_query(pg, 34,  """WITH med AS (
    SELECT
        gender,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) AS med_salary
    FROM employees
    GROUP BY gender
)
SELECT
    (SELECT med_salary FROM med WHERE gender = 'F') /
    (SELECT med_salary FROM med WHERE gender = 'M') AS gender_pay_ratio;""", "Female/Male median salary ratio.")
print(df_34)

df_35 = run_query(pg, 35, """WITH buckets AS (
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
ORDER BY r.bucket;""", 'Salary distributions (F/M) into 10 buckets')
print(df_35)

logging.info('---*Congratulations, all the queries are running correctly*---')


