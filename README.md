# Distributed Puzzle Game

This project is a distributed system implementation of a puzzle game. It consists of a server and multiple clients that communicate to solve puzzles collaboratively.

## Prerequisites

1. Python 3.10 or higher installed on your system.
2. A virtual environment set up for the project.

## Setup Instructions

### Step 1: Clone the Repository

Clone the project repository to your local machine:

```bash
git clone <repository-url>
cd distributed_puzzle_game
```

### Step 2: Set Up Virtual Environment

Create and activate a virtual environment:

```bash
python -m venv .venv
```

- On Windows:

```bash
.venv\Scripts\activate
```

- On macOS/Linux:

```bash
source .venv/bin/activate
```

### Step 3: Install Dependencies

Install the required dependencies:

```bash
pip install -r requirements.txt
```

### Step 4: Run the Server

Start the server:

```bash
python server.py --role primary --host 127.0.0.1 --port 6000
```

For backup server

```bash
python server.py --role backup --host 127.0.0.1 --port 6000
```

### Step 5: Run the Clients

Open multiple terminals and run the client script in each terminal:

```bash
python client.py --name Alice
```

### Step 6: Test the Distributed System

Interact with the clients to test the distributed system functionality. Ensure that all clients can connect to the server and communicate effectively.

## Notes

- Ensure that the server is running before starting the clients.
- If you encounter any issues, check the logs for error messages and ensure all dependencies are installed correctly.

## License

This project is licensed under the MIT License.
