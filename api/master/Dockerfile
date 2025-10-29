from python:3.11-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY api/base.py ./base.py
COPY api/master/ ./

EXPOSE ${PORT}

ENTRYPOINT ["python", "master.py"]