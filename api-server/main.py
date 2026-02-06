from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles # 정적 파일 서비스를 위해 추가!
from fastapi.responses import FileResponse   # 파일을 직접 보내주기 위해 추가!
from elasticsearch import Elasticsearch
import uvicorn
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="공공데이터 검색 API")

# [백엔드 로직] Elasticsearch 연결
es = Elasticsearch(os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200'))
INDEX_NAME = "public_metadata"

# 1. 정적 파일(HTML, CSS, JS)이 들어있는 폴더를 연결합니다.
# 이렇게 하면 /client/index.html 같은 식으로 접근이 가능해져요.
app.mount("/client", StaticFiles(directory="api-server/client"), name="client")

# 2. 홈페이지(/) 접속 시 index.html 파일을 읽어서 보내줍니다.
@app.get("/", response_class=FileResponse)
def read_index():
    return "api-server/client/index.html"

# 3. 데이터만 전문적으로 다루는 API 경로
@app.get("/search")
def search_metadata(q: str = Query(None), page: int = Query(1, ge=1)):
    """
    page: 현재 보고 싶은 페이지 번호 (기본값 1)
    """
    if not q: return {"total_found": 0, "data": [], "total_pages": 0}
    
    size = 10  # 한 페이지당 10개씩 보여주기
    start_from = (page - 1) * size  # 예: 2페이지면 (2-1)*10 = 10번 데이터부터 가져옴
    
    query = {
        "size": size,
        "from": start_from, # 어디서부터 가져올지 결정
        "query": {
            "multi_match": {
                "query": q,
                "fields": ["title", "desc"],
                "type": "phrase_prefix"
            }
        }
    }
    
    res = es.search(index=INDEX_NAME, body=query)
    
    # 전체 검색 결과 개수
    total_hits = res['hits']['total']['value']
    # 전체 페이지 수 계산 (예: 101개면 11페이지)
    total_pages = (total_hits + size - 1) // size 
    
    results = [hit['_source'] for hit in res['hits']['hits']]
    
    return {
        "keyword": q,
        "current_page": page,
        "total_pages": total_pages,
        "total_found": total_hits,
        "data": results
    }

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)