# Using the Ubuntu image (our OS)
FROM ubuntu:latest
# Update package manager (apt-get) 
# and install (with the yes flag `-y`)
# Python and Pip
RUN apt-get update && apt-get install -y \
    python3.8 \
    python3-pip

##Set current working directory
WORKDIR /apps/d2b_cap/finnhub_producer

## Copy the application files to the container
COPY . .

# Install Python dependencies
#RUN pip install -r requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


#
## Copy the application files to the container
#COPY . .
#CMD ["python","finnhub_producer/finnhub_producer.py"]