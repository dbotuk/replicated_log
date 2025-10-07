from python:3.11-slim

WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

# Copy shared base module and follower service code
COPY api/base.py ./base.py
COPY api/follower/ ./

EXPOSE 8051

ENTRYPOINT ["python", "follower.py", "0.0.0.0", "8051"]
CMD ["follower"]