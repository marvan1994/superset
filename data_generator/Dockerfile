# Use a slim Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the data generation script
COPY generate_data.py .

# The command to run when the container starts
CMD ["python", "generate_data.py"]