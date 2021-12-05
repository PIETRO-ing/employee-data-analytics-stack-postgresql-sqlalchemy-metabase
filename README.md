# SQL & docker

![](./Images/psql.png) 
![](./Images/docker.png)

The main purpose of this project is to practice docker-compose and sql.

## 1st step
Built a database called 'company' which consists of three tables:
- departments;
- regions;
- employees.
The database is dockerized and created by using sqlalchemy.

## 2nd step 
Practicing queries:
- aggregate functions;
- subqueries;
- case;
- join table.

## Structure
![](./Images/architecture.png)
- docker-compose.yml is the file that make everything possible, there are all the istruction for the containers;
- tables directory is the one responsible to create all the tables;
- queries directory will start after all the tables are created and contain all the queries.

## Let's the music start 

![](./Images/herbert_von_karajan.png)

docker-compose up (inside the directory where .yml file is located) is like Herbert Von Karajan directing the Berliner Philharmoniker, it will run all the containers.
Setting time.sleep() inside .py files is crucial to create all the tables one by one and then running the queries in this precise order.

## docker-compose commands

- docker-compose up              -----> start the entire pipeline
- docker-compose up -d           -----> start the entire pipeline in the background
- docker-compose up logs queries -----> show the output of queries
- docker-compose up -d queries   -----> run service: queries in the background
- docker-compose start queries   -----> start service queries
- docker-compose stop queries    -----> stop service queries
- docker-compose rm queries      -----> remove service queries
- docker-compose ps              -----> show running containers
- docker-compose ps -a           -----> show all containers running and not

