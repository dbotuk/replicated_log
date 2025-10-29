from python:3.11-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY api/base.py ./base.py
COPY api/follower/ ./

EXPOSE ${PORT}

ENTRYPOINT ["python", "follower.py"]
CMD ["follower", "5.0"]