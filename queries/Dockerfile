FROM python:3.8-slim


WORKDIR /app

# copy requirements file from the directory the Dockerfile is in into the /app directory of the container
COPY requirements.txt /app

# executes pip install comand and installs all required python libraries
RUN pip install -r requirements.txt

#execute tables.py script
CMD ["python", "queries.py"]