import os
import logging
from typing import Optional
from fastapi import FastAPI, Query, HTTPException, status
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# 1. 로깅 및 환경 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("MarketAPI")

load_dotenv()

app = FastAPI(title="Market Search API", version="1.2.0")

# 2. CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3. 정적 파일 마운트 (경로 계산 로직 강화)
try:
    # 현재 실행 중인 main.py의 절대 경로
    BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
    
    # [수정] main.py와 같은 층에 있는 client 폴더 지정
    CLIENT_DIR = os.path.join(BASE_DIR, "client")
    CLIENT_DIR = os.path.normpath(CLIENT_DIR)

    logger.info(f"🔎 탐색 경로 (같은 레벨): {CLIENT_DIR}")

    if os.path.exists(CLIENT_DIR):
        app.mount("/client", StaticFiles(directory=CLIENT_DIR), name="client")
        logger.info(f"✅ 정적 파일 마운트 성공: {CLIENT_DIR}")
    else:
        logger.error(f"❌ 폴더 없음: {CLIENT_DIR}")
        # 혹시 모를 오타 대비 리스트 출력
        logger.info(f"현재 폴더 목록: {os.listdir(BASE_DIR)}")
            
except Exception as e:
    logger.error(f"❌ 마운트 처리 중 예외 발생: {e}")

# 4. Elasticsearch 연결
try:
    ES_SERVER = os.getenv('ES_SERVER', 'http://127.0.0.1:9200')
    ES_INDEX = "market-info"
    es = Elasticsearch([ES_SERVER], request_timeout=30)
except Exception as e:
    logger.critical(f"❌ ES 초기화 에러: {e}")
    es = None

# 5. 검색 엔드포인트
@app.get("/search")
async def search_markets(q: Optional[str] = Query(None), page: int = Query(1)):
    if es is None or not es.ping():
        raise HTTPException(status_code=503, detail="Elasticsearch 연결 불가")
    
    try:
        size = 10
        from_idx = (page - 1) * size

        # 검색어가 없을 때는 전체 노출, 있을 때는 와일드카드 검색
        if not q or not q.strip():
            search_query = {"match_all": {}}
        else:
            search_query = {
                "query_string": {
                    "query": f"*{q}*", 
                    "fields": ["market_name^3", "address_road", "address_jibun", "items_sold"],
                    "analyze_wildcard": True
                }
            }

        response = es.search(
            index=ES_INDEX,
            query=search_query,
            from_=from_idx,
            size=size
        )

        hits = response['hits']['hits']
        total_value = response['hits']['total']['value']
        
        # 실제 데이터가 있는지 로그로 확인
        logger.info(f"🔍 검색어 '{q}' 결과: {len(hits)}건 발견 (전체 {total_value}건)")

        return {
            "items": [hit['_source'] for hit in hits],
            "has_more": total_value > (from_idx + size),
            "total": total_value
        }

    except Exception as e:
        logger.error(f"🚨 검색 오류: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    # uvicorn 실행 시 현재 위치를 고정하기 위해 명시적 호출
    uvicorn.run(app, host="0.0.0.0", port=8000)