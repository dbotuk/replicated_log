from python:3.11-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY api/base.py ./base.py
COPY api/follower/ ./

EXPOSE 8051

ENTRYPOINT ["python", "follower.py", "0.0.0.0", "8051"]
CMD ["follower"]