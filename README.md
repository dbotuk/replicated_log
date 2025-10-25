# Distributed Systems Homework

This project implements a distributed system with one master server and two follower servers for message replication.

## System Architecture

- **Master server** (port 6061) - receives messages and replicates them to follower servers
- **Follower-1 server** (port 6062) - replication with 5.0 second delay
- **Follower-2 server** (port 6063) - replication with 2.0 second delay

## Installation and Setup

### Prerequisites
- Docker
- Docker Compose

### Running the System

1. Clone the repository:
```bash
git clone <repository-url>
cd replicated_log
```

2. Create a `.env` file with the following variables:
```bash
MASTER_PORT=6061
FOLLOWER_1_PORT=6062
FOLLOWER_2_PORT=6063
```

3. Start the system:
```bash
docker-compose up --build
```

## API Endpoints

### Master Server (port 6061)

#### POST /add
Adds a new message and replicates it to all follower servers.

**Request:**
```json
{
  "data": "message content",
  "write_concern": 1
}
```

**Response:**
```json
{
  "status_code": 200,
  "message": "Message added successfully",
  "data": null
}
```

#### GET /messages
Retrieves all messages from the master server.

**Response:**
```json
{
  "status_code": 200,
  "message": "Messages fetched successfully",
  "data": [
    {
      "text": "message content"
    }
  ]
}
```

### Follower Servers (ports 6062, 6063)

#### POST /replicate
Internal endpoint for message replication from the master server.

#### GET /messages
Retrieves all replicated messages from the follower server.

## System Testing

### 1. Adding a message
```bash
curl -X POST -H "Content-Type: application/json" http://localhost:6061/add -d '{"data": "msg1", "write_concern": 1}'
```

### 2. Checking messages on master server
```bash
curl http://localhost:6061/messages
```

### 3. Checking messages on follower-1 server
```bash
curl http://localhost:6062/messages
```

### 4. Checking messages on follower-2 server
```bash
curl http://localhost:6063/messages
```

## Complete Testing Example

1. **Add several messages:**
```bash
curl -X POST -H "Content-Type: application/json" http://localhost:6061/add -d '{"data": "msg1", "write_concern": 1}'
curl -X POST -H "Content-Type: application/json" http://localhost:6061/add -d '{"data": "msg2", "write_concern": 1}'
curl -X POST -H "Content-Type: application/json" http://localhost:6061/add -d '{"data": "msg3", "write_concern": 1}'
```

2. **Check replication on all servers:**
```bash
# Master server
curl http://localhost:6061/messages

# Follower-1 server (5.0s delay)
curl http://localhost:6062/messages

# Follower-2 server (2.0s delay)
curl http://localhost:6063/messages
```

## Implementation Features

- **Parallel replication**: Master server replicates messages to all follower servers simultaneously
- **Different delays**: Follower-1 has 5.0 second delay, Follower-2 has 2.0 second delay
- **Thread-safe**: Uses locks for safe message handling
- **Logging**: Detailed logging of all operations for monitoring

## Monitoring

View logs to track the replication process:
```bash
docker-compose logs -f
```

## Stopping the System

```bash
docker-compose down
```

## Project Structure

```
api/
├── base.py              # Base data models
├── master/
│   ├── Dockerfile       # Docker configuration for master
│   └── master.py        # Master server
├── follower/
│   ├── Dockerfile       # Docker configuration for follower
│   └── follower.py      # Follower server
docker-compose.yaml      # Docker Compose configuration
requirements.txt         # Python dependencies
```
