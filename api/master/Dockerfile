from python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching, then install
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

# Copy shared base module and master service code
COPY api/base.py ./base.py
COPY api/master/ ./

EXPOSE 8051

ENTRYPOINT ["python", "master.py", "0.0.0.0", "8051"]
CMD ["http://0.0.0.0:8052"]