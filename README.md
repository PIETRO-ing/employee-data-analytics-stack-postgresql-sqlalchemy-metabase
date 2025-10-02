# Employee Data Analytics Stack with Docker, SQL, and Metabase

This project sets up a containerized data stack using **Docker Compose, PostgreSQL, SQLAlchemy,** and **Metabase.**
It demonstrates how to design a relational database, perform SQL-based analysis, and create interactive dashboards, all within a reproducible, containerized environment.

## Project Structure

| Component            | Description                                                 |
| -------------------- | ----------------------------------------------------------- |
| `tables/`            | Creates the PostgreSQL database and tables using SQLAlchemy |
| `queries/`           | Runs SQL queries on the created database                    |
| `metabase`           | Data visualization UI connected to the PostgreSQL database  |
| `docker-compose.yml` | Coordinates all services and defines container behavior     |

---

## Step-by-Step Breakdown

### 1. Build the Database

A PostgreSQL database named `company` is created using SQLAlchemy with three tables:

* `departments`
* `regions`
* `employees`

This is handled by the `tables` container and SQLAlchemy code.

### 2. Run SQL Queries

After the tables are built, the `queries` service executes various types of SQL queries, including:

* Aggregate functions
* Joins
* Subqueries
* CASE expressions

### 3. Visualize with Metabase

Metabase connects to the PostgreSQL database and provides a web UI to explore data and build dashboards.

---

## How to Use It

### First Time Setup (Build Everything)

Before running the command below it's essential to create a `.env` file in the same folder of the `.yml` file with the following content:

- POSTGRES_USER
- POSTGRES_PASSWORD
- POSTGRES_DB

```bash
docker-compose up --build
```

This will:

* Start PostgreSQL
* Create the schema (`tables`)
* Run queries
* Launch Metabase

Access Metabase at: [http://localhost:3000](http://localhost:3000)

> ⚠️ The first time you visit Metabase, you'll create an admin account and connect to the PostgreSQL database:
>
> * **Database:** PostgreSQL
> * **Host:** `postgresdb`
> * **Port:** `5432`
> * **Database name:** from `.env`
> * **Username:** from `.env`
> * **Password:** from `.env`

---

### 🟢 Start Project Normally (Skip Rebuilding DB)

Once your DB is built, you can start only the necessary containers:

```bash
docker-compose up postgresdb metabase queries
```

If you don't want to run queries:

```bash
docker-compose up postgresdb metabase
```

Or run in the background:

```bash
docker-compose up -d postgresdb metabase
```

---

### 🛑 Stop Everything

```bash
docker-compose down
```

> ⚠️ Do **not** use `-v` unless you want to delete all data (including dashboards).

---

## Metabase Persistence

* Metabase data (users, dashboards, settings) is persisted in `metabase-data:/metabase.db`
* PostgreSQL uses Docker volumes, so your data will persist even if the containers are stopped or restarted.

✅ Your dashboards and admin login will **persist between sessions**

---

## Useful Commands

Here are common 'docker-compose' commands to manage the project

| Command                           | What it does                  |
| --------------------------------- | ----------------------------- |
| `docker-compose up`               | Starts all services           |
| `docker-compose up --build`       | Rebuilds images and starts    |
| `docker-compose up -d`            | Starts in background          |
| `docker-compose down`             | Stops and removes containers  |
| `docker-compose ps`               | Shows container status        |
| `docker-compose logs -f metabase` | Shows logs for Metabase       |
| `docker-compose up queries`       | Runs just the queries service |
| `docker-compose up tables`        | Rebuilds schema manually      |

---

## Connect to DB Manually (Optional)

If you've mapped port  `5555` to the PostgreSQL container, you can connect manually using:

```bash
psql -U postgres -d company -h localhost -p 5555
```

---

## Notes

* `time.sleep()` is used to control execution order in services
* `tables` should only be run once or when rebuilding the schema
* `metabase-data/` contains persistent state for dashboards and users

---

## Dashboards with Metabase

* Employees per department
* Avg salary per region (if salary data is added)
* New hires over time (if hire date is added)
* Interactive filters by department or region

---


