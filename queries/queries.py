import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import time

time.sleep(100)
print('Hello!\nConnecting!')

# Read DB connection info from environment variables
db_user = os.getenv('POSTGRES_USER')
db_pass = os.getenv('POSTGRES_PASSWORD')
#db_host = os.getenv('POSTGRES_HOST', 'postgresdb')  # Default to Docker service name
db_name = os.getenv('POSTGRES_DB')

# Create the SQLAlchemy engine
pg = create_engine(f'postgresql://{db_user}:{db_pass}@postgresdb/{db_name}', echo=True)


# Just to see if everything is working
query_00 = """select * 
              from regions;"""

with pg.connect() as conn:
    df_00 = pd.read_sql(query_00, pg)
    print(df_00)

# Queries
print('\n\n\n---*Top 5 highest paid employees*---')
query_01 = """select * from employees
               where salary > 100000
               order by salary desc
               limit 5;"""

with pg.connect() as conn:
    df_01 = pd.read_sql(query_01, pg)
    print(df_01)

print('\n\n\n---*Total salary broken by department and gender*---')
query_02 = """select department, gender, sum(salary)
from employees
group by department, gender
order by sum(salary) desc;"""

with pg.connect() as conn:
    df_02 = pd.read_sql(query_02,pg)
    print(df_02)

print('\n\n\n---*How many employees without an email?*---')
query_03 = """select count(*)
from employees
where email is NULL;"""

with pg.connect() as conn:
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

with pg.connect() as conn:
    df_04 = pd.read_sql(query_04, pg)
    print(df_04)

print('\n\n\n---*Transposing the previous query*---')
query_05 = """select sum ( case when salary < 100000 then 1 else 0  end) as UNDER_PAID,
sum ( case when salary > 100000 and salary < 160000 then 1 else 0 end) as PAID_WELL,
sum( case when salary > 160000 then 2 else 0 end) as EXECUTIVE
from employees;"""

with pg.connect() as conn:
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

with pg.connect() as conn:
    df_06 = pd.read_sql(query_06, pg)
    print(df_06)

print('\n\n\n')
query_07 = """select first_name||' '||last_name full_name, country
from  employees inner join regions
on employees.region_id = regions.region_id;"""

with pg.connect() as conn:
    df_07 = pd.read_sql(query_07, pg)
    print(df_07)

print('\n\n\n')
query_08 = """select first_name||' '||last_name, email, division, country
from employees inner join departments
on employees.department = departments.department
inner join regions on employees.region_id = regions.region_id --join w/ the result of the others
where email is not null;"""

with pg.connect() as conn:
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

with pg.connect() as conn:
    df_09 = pd.read_sql(query_09, pg)
    print(df_09)

print('\n\n\n')
query_10 = """select first_name||' '||last_name full_name, department, count(*) over(partition by department)
from employees;"""

with pg.connect() as conn:
    df_10 = pd.read_sql(query_10, pg)
    print(df_10)

print('\n\n\n')
query_11 = """select department, count(*) as total_employees, sum(salary) as total_salary
from employees
group by department
union all
select 'TOTAL', count(*), sum(salary)
from employees;"""

with pg.connect() as conn:
    df_11 = pd.read_sql(query_11, pg)
    print(df_11)

print('\n\n\n---*The total salary paid by each department?*---')
query_12 = """select department, sum(salary) as total_salary
              from employees
              group by department
              order by total_salary desc;"""

with pg.connect() as conn:
    df_12 = pd.read_sql(query_12, pg)
    print(df_12)

print('\n\n\n')
query_13 = """select first_name||' '||last_name full_name, department, sum(salary) over(partition by department) total_salary
              from employees
              order by total_salary desc;"""

with pg.connect() as conn:
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

with pg.connect() as conn:
    df_14 = pd.read_sql(query_14, pg)
    print(df_14)

print('\n\n\n---*rank employees table over department based on salary highest to low*---')
query_15 = """select first_name, last_name, department, salary,
              rank() over(partition by department order by salary desc)
              from employees;"""

with pg.connect() as conn:
    df_15 = pd.read_sql(query_15, pg)
    print(df_15)

print('\n\n\n---*All the highest paid employees for each department*---')
query_16 = """select a.first_name||' '||a.last_name full_name, a.department, a.salary
              from (select first_name, last_name, department, salary,
              rank() over(partition by department order by salary desc)
              from employees) a
              where rank = 1
              order by salary desc;"""

with pg.connect() as conn:
    df_16 = pd.read_sql(query_16, pg)
    print(df_16)

print('\n\n\n---*creat a two column with lead and lag*---')
query_17 = """select first_name||' '||last_name full_name, department,
              rank() over(partition by department order by salary desc) as salary_rank,
              salary,
              lead(salary) over() as next_salary,
              lag(salary) over() as previous_salary
              from employees;"""

with pg.connect() as conn:
    df_17 = pd.read_sql(query_17, pg)
    print(df_17)

print('\n\n\n')
query_18 = """select first_name||' '||last_name full_name, department, salary,
              lead(salary) over(order by salary desc) as closest_lower_employee,
              lag(salary) over(order by salary desc) as closest_higher_employee
              from employees;"""

with pg.connect() as conn:
    df_18 = pd.read_sql(query_18, pg)
    print(df_18)

print('\n\n\n')
query_19 = """select first_name||' '||last_name full_name, department, salary,
              lead(salary) over(partition by department order by salary desc) as closest_lower_employee,
              lag(salary) over(partition by department order by salary desc) as closest_higher_employee
              from employees;"""

with pg.connect() as conn:
    df_19 = pd.read_sql(query_19, pg)
    print(query_19)

print('\n\n\n---*how many employees with the same first name?*---')
query_20 = """select first_name, count(*) count_name
              from employees
              group by first_name
              having count(*) != 1
              order by count(*) desc;"""

with pg.connect() as conn:
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

with pg.connect() as conn:
    df_21 = pd.read_sql(query_21, pg)
    print(df_21)

print("\n\n\n---*how many employees in Sports, Tools, Clothing, Computers department? show in a transposed way*---")
query_22 = """select sum( case when department = 'Sports' then 1 else 0 end ) as Sports,
              sum( case when department = 'Tools' then 1 else 0 end ) as Tools,
              sum( case when department = 'Clothing' then 1 else 0 end ) as Clothing,
              sum( case when department = 'Computers' then 1 else 0 end ) as Computers
              from employees;"""

with pg.connect() as conn:
    df_22 = pd.read_sql(query_22, pg)
    print(df_22)

print('\n\n\n---*how many employees with the same email domain?*---')
query_23 = """select a.email_domain, count(*)
              from (select substring(email, position('@' in email)+1) as email_domain
              from employees)a
              where a.email_domain is not null
              group by a.email_domain
              order by count(*) desc;"""

with pg.connect() as conn:
    df_23 = pd.read_sql(query_23, pg)
    print(df_23)

print('\n\n\n---*min, max, avg salary broken down by gender, region, country*---')
query_24 = """select e.gender gender, e.region_id id_region, r.region||r.country country,min(salary) min, max(salary) max, round(avg(salary)) avg
              from employees e inner join regions r on e.region_id = r.region_id
              group by gender, id_region, r.region||r.country
              order by gender;"""

with pg.connect() as conn:
    df_24 = pd.read_sql(query_24, pg)
    print(df_24)

print('\n\n\n')
query_25 = """select department,
              replace(department, 'Clothing', 'XXXXX') modified_dept,
              department||' '|| 'department' complete_dept_name
              from employees;"""

with pg.connect() as conn:
    df_25 = pd.read_sql(query_25, pg)
    print(df_25)

print('\n\n\n')
query_26 = """select first_name||' '||last_name, department, sum(salary) over(partition by department), salary ,
 (salary - (select max(salary) from employees)) less_sal
from employees;"""

with pg.connect() as conn:
    df_26 = pd.read_sql(query_26, pg)
    print(df_26)

print('\n\n\n---*Congratulations, all the queries are running correctly*---')


