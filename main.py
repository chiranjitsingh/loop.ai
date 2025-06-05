from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from enum import Enum
import uuid
import time
import heapq
from typing import List, Dict
import threading

app = FastAPI()

# Priority Enum
class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

# Request Model
class IngestionRequest(BaseModel):
    ids: List[int]
    priority: Priority

# Status Storage
ingestion_statuses: Dict[str, dict] = {}
batch_queue = []
processing_lock = threading.Lock()
last_processed = 0

# Priority Queue Functions
def add_to_queue(ingestion_id: str, batch: list, priority: Priority, created: float):
    priority_value = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}[priority]
    heapq.heappush(batch_queue, (priority_value, created, ingestion_id, batch))

def get_next_batch():
    if batch_queue:
        return heapq.heappop(batch_queue)
    return None

# API Endpoints
@app.post("/ingest")
async def ingest_data(request: IngestionRequest):
    ingestion_id = str(uuid.uuid4())
    batches = [request.ids[i:i+3] for i in range(0, len(request.ids), 3)]
    
    ingestion_statuses[ingestion_id] = {
        "status": "yet_to_start",
        "batches": [
            {
                "batch_id": str(uuid.uuid4()),
                "ids": batch,
                "status": "yet_to_start"
            } for batch in batches
        ],
        "created": time.time()
    }
    
    for batch in ingestion_statuses[ingestion_id]["batches"]:
        add_to_queue(ingestion_id, batch["ids"], request.priority, ingestion_statuses[ingestion_id]["created"])
    
    if not hasattr(app, 'worker_thread') or not app.worker_thread.is_alive():
        app.worker_thread = threading.Thread(target=process_batches)
        app.worker_thread.daemon = True
        app.worker_thread.start()
    
    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
async def get_status(ingestion_id: str):
    if ingestion_id not in ingestion_statuses:
        raise HTTPException(status_code=404, detail="Ingestion ID not found")
    
    status = ingestion_statuses[ingestion_id]
    overall_status = "completed"
    for batch in status["batches"]:
        if batch["status"] == "triggered":
            overall_status = "triggered"
            break
        elif batch["status"] == "yet_to_start":
            overall_status = "yet_to_start"
    
    return {
        "ingestion_id": ingestion_id,
        "status": overall_status,
        "batches": status["batches"]
    }

# Batch Processing
def process_batches():
    global last_processed
    
    while True:
        with processing_lock:
            current_time = time.time()
            if current_time - last_processed < 5:
                time.sleep(5 - (current_time - last_processed))
            
            next_batch = get_next_batch()
            if not next_batch:
                time.sleep(1)
                continue
            
            _, _, ingestion_id, batch_ids = next_batch
            
            # Update status to triggered
            for b in ingestion_statuses[ingestion_id]["batches"]:
                if set(b["ids"]) == set(batch_ids):
                    b["status"] = "triggered"
                    break
            
            # Simulate processing
            time.sleep(2)  # Simulate API calls
            
            # Update status to completed
            for b in ingestion_statuses[ingestion_id]["batches"]:
                if set(b["ids"]) == set(batch_ids):
                    b["status"] = "completed"
                    break
            
            last_processed = time.time()