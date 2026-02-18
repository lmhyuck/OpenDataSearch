import os
import json
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers
from pydantic import BaseModel, Field, ValidationError, ConfigDict

# 1. 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("MarketStreamProcessor")

load_dotenv()

# 2. 전역 스키마 정의 (Collector의 원본 필드를 표준 필드명으로 매핑)
class MarketData(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    # [매핑] 원본(alias) -> 시스템 표준 필드명
    market_id: str = Field(..., alias="bizesId")
    market_name: str = Field(..., alias="bizesNm")
    address_road: str = Field(..., alias="rdnmAdr")
    address_jibun: str = Field(..., alias="lnoAdr")
    latitude: str = Field(..., alias="lat")
    longitude: str = Field(..., alias="lon")
    items_sold: str = Field("정보없음", alias="indsLclsNm")

    # Collector에서 추가한 메타데이터
    region_code: Optional[str] = None
    collected_at: Optional[str] = None
    source: Optional[str] = "SBiz_API"

# 3. 환경 변수 및 인프라 설정
KAFKA_SERVER = os.getenv('KAFKA_SERVER', '127.0.0.1:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'market-data')
ES_SERVER = os.getenv('ES_SERVER', 'http://127.0.0.1:9200')
ES_INDEX = "market-info"

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        group_id=os.getenv('KAFKA_GROUP_ID', 'market-processor-group-vfinal'),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    es = Elasticsearch([ES_SERVER], request_timeout=30)
    if not es.ping():
        raise ConnectionError("Elasticsearch 연결 실패")
    
    logger.info("✅ Processor 인프라 연결 완료 (Kafka & ES)")
except Exception as e:
    logger.critical(f"❌ 초기화 오류: {e}")
    exit(1)

def transform_and_index(messages):
    """메시지 묶음을 변환하여 ES에 벌크 인덱싱 (중복 방지 포함)"""
    actions = []
    for msg_value in messages:
        try:
            # Pydantic을 통한 데이터 검증 및 필드명 정규화
            validated = MarketData(**msg_value)
            doc = validated.model_dump()
            
            # ES Bulk 포맷 생성 (id를 market_id로 지정하여 중복 방지)
            actions.append({
                "_op_type": "index",
                "_index": ES_INDEX,
                "_id": validated.market_id,
                "_source": doc
            })
        except ValidationError as ve:
            logger.warning(f"⚠️ 데이터 검증 실패 스킵")
        except Exception as e:
            logger.error(f"❌ 개별 메시지 처리 중 오류: {e}")

    if actions:
        helpers.bulk(es, actions)
        logger.info(f"🚀 {len(actions)}건의 데이터 동기화 완료 (Upsert)")

def main():
    logger.info("📡 스트림 프로세싱 가동 중...")
    batch = []
    try:
        for message in consumer:
            batch.append(message.value)
            
            # 100건씩 모아서 처리하거나 5초마다 처리 (성능 최적화)
            if len(batch) >= 100:
                transform_and_index(batch)
                batch = []
                
    except KeyboardInterrupt:
        logger.info("정지 명령 수신")
    finally:
        if batch:
            transform_and_index(batch)
        consumer.close()
        logger.info("안전하게 종료되었습니다.")

if __name__ == "__main__":
    main()