# AutoAI Platform

AI-driven data platform for metadata extraction and querying.

## Features

- Trino connection management
- Project-based metadata management
- Automated metadata extraction
- FastAPI-based REST API

## Requirements

- Python 3.8+
- PostgreSQL
- Trino

## Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
cd YOUR_REPO_NAME
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Usage

Start the API server:
```bash
uvicorn src.api.main:app --reload
```

The API will be available at `http://localhost:8000`

## API Documentation

Once the server is running, visit:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
