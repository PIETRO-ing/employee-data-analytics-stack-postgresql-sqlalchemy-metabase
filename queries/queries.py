import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import time

time.sleep(250)
print('Hello!\nConnecting!')
# make the connection
pg = create_engine('postgresql://postgres:pwd@postgresdb/company', echo=True)

# just to see if everything is working
query_00 = """select * 
              from regions;"""
df_00 = pd.read_sql(query_00, pg)
print(df_00)

# highest paid employees
query_01 = """select * from employees
               where salary > 100000
               order by salary desc
               limit 5;"""
df_01 = pd.read_sql(query_01, pg)
print(df_01)

query_02 = """select department, gender, sum(salary)
from employees
group by department, gender
order by sum(salary) desc;"""
df_02 = pd.read_sql(query_02,pg)
print(df_02)

query_03 = """select count(*)
from employees
where email is NULL;"""
df_03 = pd.read_sql(query_03, pg)
print(df_03)


query_04 = """select a.category, count(*)
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
df_04 = pd.read_sql(query_04, pg)
print(df_04)

#transposing a table
query_05 = """select sum ( case when salary < 100000 then 1 else 0  end) as UNDER_PAID,
sum ( case when salary > 100000 and salary < 160000 then 1 else 0 end) as PAID_WELL,
sum( case when salary > 160000 then 2 else 0 end) as EXECUTIVE
from employees;"""
df_05 = pd.read_sql(query_05, pg)
print(df_05)

query_06 = """select first_name, case when region_id = 1 then 'USA' end as region_1_SW_USA,
case when region_id = 2 then 'USA' end as region_2_NE_USA,
case when region_id = 3 then 'USA' end as region_3_NW_USA,
case when region_id = 4 then 'ASIA' end as region_4_CENTRAL_ASIA,
case when region_id = 5 then 'ASIA' end as region_5_EAST_ASIA,
case when region_id = 6 then 'CANADA' end as region_6_QUEBEC_CANADA,
case when region_id = 7 then 'CANADA' end as region_7_NOVA_SCOTIA_CANADA
from employees;"""
df_06 = pd.read_sql(query_06, pg)
print(df_06)

query_07 = """select first_name, country
from  employees inner join regions
on employees.region_id = regions.region_id;"""
df_07 = pd.read_sql(query_07, pg)
print(df_07)

query_08 = """select first_name, email, division, country
from employees inner join departments
on employees.department = departments.department
inner join regions on employees.region_id = regions.region_id --join w/ the result of the others
where email is not null;"""
df_08 = pd.read_sql(query_08, pg)
print(df_08)

query_09 = """select first_name, last_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id
where hire_date = (select max(hire_date) from (select first_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id)a)
union all
(select first_name, last_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id
where hire_date = (select min(hire_date) from (select first_name, last_name, department, hire_date, country
from employees inner join regions on employees.region_id = regions.region_id) b)
limit 1)
order by hire_date;"""
df_09 = pd.read_sql(query_09, pg)
print(df_09)

query_10 = """select first_name, department, count(*) over(partition by department)
from employees;"""
df_10 = pd.read_sql(query_10, pg)
print(df_10)

query_11 = """select department, count(*) as total_employees, sum(salary) as total_salary
from employees
group by department
union all
select 'TOTAL', count(*), sum(salary)
from employees;"""
df_11 = pd.read_sql(query_11, pg)
print(df_11)

print('The total salary paid by each department?')
query_12 = """select department, sum(salary) as total_salary
              from employees
              group by department
              order by total_salary desc;"""
df_12 = pd.read_sql(query_12, pg)
print(df_12)

query_13 = """select first_name, last_name, department, sum(salary) over(partition by department) total_salary
              from employees
              order by total_salary desc;"""
df_13 = pd.read_sql(query_13, pg)
print(df_13)

print('how many under paid, paid well or executiv are there?')
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

print('rank employees table over department based on salary highest to low')
query_15 = """select first_name, last_name, department, salary,
              rank() over(partition by department order by )
              from employees"""
df_15 = pg.read_sql(query_15, pg)
print(df_15)

print('All the highest paid employees for each department')
query_16 = """select a.first_name, a.last_name, a.department, a.salary
              from (select first_name, last_name, department, salary,
              rank() over(partition by department order by salary desc)
              from employees) a
              where rank = 1
              order by salary desc;"""
df_16 = pg.read_sql(query_16, pg)
print(df_16)

print('creat a two column with lead and lag')
query_17 = """select first_name, last_name, department,
              rank() over(partition by department order by salary desc) as salary_rank,
              salary,
              lead(salary) over() as next_salary,
              lag(salary) over() as previous_salary
              from employees;"""
df_17 = pg.read_sql(query_17, pg)

