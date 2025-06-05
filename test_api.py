import pytest
from fastapi.testclient import TestClient
from main import app
import time

client = TestClient(app)

def test_ingest_endpoint():
    response = client.post("/ingest", json={"ids": [1,2,3,4,5], "priority": "MEDIUM"})
    assert response.status_code == 200
    assert "ingestion_id" in response.json()

def test_status_endpoint():
    # First create an ingestion
    ingest_response = client.post("/ingest", json={"ids": [1,2,3], "priority": "HIGH"})
    ingestion_id = ingest_response.json()["ingestion_id"]
    
    # Check status
    status_response = client.get(f"/status/{ingestion_id}")
    assert status_response.status_code == 200
    assert status_response.json()["ingestion_id"] == ingestion_id

def test_priority_handling():
    # Create medium priority request
    medium_resp = client.post("/ingest", json={"ids": [1,2,3,4,5], "priority": "MEDIUM"})
    time.sleep(1)  # Ensure different creation time
    
    # Create high priority request
    high_resp = client.post("/ingest", json={"ids": [6,7,8], "priority": "HIGH"})
    
    # Give time for processing
    time.sleep(6)
    
    # Check high priority was processed first
    high_status = client.get(f"/status/{high_resp.json()['ingestion_id']}").json()
    assert any(b["status"] == "completed" for b in high_status["batches"])