import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
from elasticsearch import Elasticsearch
from pymongo import MongoClient

from services.query_service.config import QueryServiceConfig

class SearchResponse(BaseModel):
    results: List[Dict[str, Any]]
    total: int

config = QueryServiceConfig.from_env()
app = FastAPI(title="The Muezzin - Query Service")

def get_es() -> Elasticsearch:
    "Return Elasticsearch connection"
    return Elasticsearch(
        hosts=[{
            "host": config.elasticsearch_host,
            "port": int(config.elasticsearch_port),
            "scheme": "http"
        }],
        verify_certs=False
    )

def get_mongo():
    "Return MongoDB collection"
    client = MongoClient(config.mongodb_uri)
    return client[config.mongodb_db_name][config.mongodb_collection_name]

@app.get("/health")
def health():
    "Health check endpoint"
    return {"status": "ok"}

@app.get("/search/files", response_model=SearchResponse)
def search_files(q: Optional[str] = Query(None, description="Free text query on metadata and transcription"), size: int = 25, from_: int = 0):
    "Search files by metadata and transcription"
    es = get_es()
    if not q:
        query = {"match_all": {}}
    else:
        # lenient avoids numeric/date parsing errors on wildcard fields like metadata.*
        query = {
            "multi_match": {
                "query": q,
                "fields": ["transcription", "metadata.*"],
                "lenient": True
            }
        }
    resp = es.search(
        index=config.elasticsearch_index_files,
        from_=from_,
        size=size,
        query=query
    )
    hits = [h.get("_source", {}) for h in resp.get("hits", {}).get("hits", [])]
    total = resp.get("hits", {}).get("total", {}).get("value", 0)
    return {"results": hits, "total": total}

@app.get("/search/transcriptions", response_model=SearchResponse)
def search_transcriptions(q: Optional[str] = Query(None, description="Search in transcriptions only"), size: int = 25, from_: int = 0):
    "Search transcriptions only"
    es = get_es()
    query = {"match_all": {}} if not q else {"match": {"transcription": q}}
    resp = es.search(
        index=config.elasticsearch_index_files,
        from_=from_,
        size=size,
        query=query
    )
    hits = [h.get("_source", {}) for h in resp.get("hits", {}).get("hits", [])]
    total = resp.get("hits", {}).get("total", {}).get("value", 0)
    return {"results": hits, "total": total}

@app.get("/bds/search", response_model=SearchResponse)
def search_bds(min_percentage: Optional[float] = Query(None), threat_level: Optional[str] = Query(None), size: int = 25, from_: int = 0):
    "Search BDS by percentage and threat level"
    es = get_es()
    must = []
    if min_percentage is not None:
        must.append({"range": {"bds_percentage": {"gte": min_percentage}}})
    if threat_level:
        must.append({"term": {"bds_threat_level.keyword": threat_level}})
    query = {"bool": {"must": must}} if must else {"match_all": {}}
    resp = es.search(
        index=config.elasticsearch_index_bds,
        from_=from_,
        size=size,
        query=query
    )
    hits = [h.get("_source", {}) for h in resp.get("hits", {}).get("hits", [])]
    total = resp.get("hits", {}).get("total", {}).get("value", 0)
    return {"results": hits, "total": total}


@app.get("/files/recent", response_model=SearchResponse)
def recent_files(size: int = 25, from_: int = 0):
    "Return most recent files by timestamp (if present), else fallback to match_all"
    es = get_es()
    try:
        resp = es.search(
            index=config.elasticsearch_index_files,
            from_=from_,
            size=size,
            sort=[{"timestamp": "desc"}],
            query={"match_all": {}}
        )
    except Exception:
        resp = es.search(index=config.elasticsearch_index_files, from_=from_, size=size, query={"match_all": {}})
    hits = [h.get("_source", {}) for h in resp.get("hits", {}).get("hits", [])]
    total = resp.get("hits", {}).get("total", {}).get("value", 0)
    return {"results": hits, "total": total}

@app.get("/files/{doc_id}")
def get_file(doc_id: str):
    "Get file document and binary presence"
    es = get_es()
    try:
        es_doc = es.get(index=config.elasticsearch_index_files, id=doc_id)
        source = es_doc.get("_source", {})
    except Exception:
        source = {}
    coll = get_mongo()
    mongo_has_data = bool(
        coll.find_one({"id": doc_id, "data": {"$exists": True, "$ne": None}}, {"_id": 1})
    )
    return {"document": source, "has_binary": mongo_has_data}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "services.query_service.main:app",
        host=config.api_host,
        port=config.api_port,
        reload=False
    )