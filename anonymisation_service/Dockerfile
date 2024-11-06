#In the future when we do the complete implementation this will be the setup, 
#for now this is sample Dockerfile this is not used or tested for prototype.
# Use the official Prefect image as the base
FROM prefecthq/prefect:2-python3.11

# Set environment variable for the environment
ARG env
ENV environment=$env

# Copy requirements and install dependencies
COPY requirements.txt /opt/prefect/anonymisation_service/requirements.txt
RUN python -m pip install -r /opt/prefect/anonymisation_service/requirements.txt

# Copy the rest of the application code
COPY . /opt/prefect/anonymisation_service

# Set the working directory
WORKDIR /opt/prefect/anonymisation_service
