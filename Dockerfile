# Use the official Python image from the Docker Hub
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y telnet && apt-get clean
RUN apt-get update && apt-get install -y nano && apt-get clean

# Make port 3306 available to the world outside this container (for MySQL connection)
EXPOSE 3306

# Run binlog_processor.py when the container launches
CMD ["python", "binlog_processor.py"]