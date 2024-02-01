# Use an official Python runtime as base image
FROM python:3.9.18-bookworm

# Set the working directory inside the container
WORKDIR /usr/src/app

RUN pip install --upgrade pip

# Copy the requirements.txt into the container
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r ops/requirements.txt