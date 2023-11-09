# Use python 3.8 base image from Docker Hub
FROM python:3.8

# Install required packages for Python
RUN apk update && apk add python3.8 py3-pip

#Set current working directory
WORKDIR /apps/app_d2b/intermediate/cap_project/finnhub_producer

# Copy the application files to the container
COPY . .

# Install Python dependencies
#RUN pip install -r requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
#ENV FLASK_APP=app.py

# Expose port 80 to the outside
#EXPOSE 80

# Run the frontend application
#CMD ["flask", "run", "--host=0.0.0.0", "--port=80"]


CMD ["python","producer_finnhub/main.py"]