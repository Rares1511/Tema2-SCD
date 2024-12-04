FROM python:3.6
COPY requirements.txt /tmp
RUN pip install -U setuptools
RUN pip install -r /tmp/requirements.txt
RUN apt-get update && apt-get install -y iputils-ping curl && rm -rf /var/lib/apt/lists/*
COPY /src /app
WORKDIR /app
EXPOSE 80
CMD ["python", "main.py"]