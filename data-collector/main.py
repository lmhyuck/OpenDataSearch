import os
import requests
import json
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from kafka import KafkaProducer

# 1. 로깅 전략
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("MarketCollector")

load_dotenv()

API_URL = os.getenv('MARKET_API_URL')
API_KEY = os.getenv('MARKET_API_KEY')
KAFKA_SERVER = os.getenv('KAFKA_SERVER', '127.0.0.1:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'market-data')
TARGET_REGION_CODES = ['36', '11', '26']

def get_kafka_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks=1, retries=5
        )
    except Exception as e:
        logger.critical(f"❌ Kafka 연결 실패: {e}")
        return None

def fetch_page(region_code: str, page_no: int) -> Optional[List[Dict[str, Any]]]:
    """JSON/XML 응답을 모두 처리하는 방어적 수집 함수"""
    if not API_URL or not API_KEY: 
        logger.error("❌ API 설정 정보가 없습니다.")
        return None

    params = {
        'serviceKey': requests.utils.unquote(API_KEY),
        'pageNo': page_no,
        'numOfRows': 1000,
        'divId': 'ctprvnCd',
        'key': region_code,
        'resType': 'json'
    }

    try:
        # [수정] false -> False (Python 문법 준수)
        response = requests.get(API_URL, params=params, timeout=30, verify=False)
        raw_text = response.text.strip()

        if not raw_text:
            logger.warning(f"⚠️ 빈 응답 수신: Region[{region_code}]")
            return None

        # 1. JSON 파싱 시도
        if raw_text.startswith('{'):
            try:
                data = response.json()
                items = data.get('body', {}).get('items', [])
                if items: return items
            except Exception:
                pass

        # 2. XML 파싱 시도
        if raw_text.startswith('<'):
            logger.info(f"⚡ XML 형식 감지, 파싱 시작...")
            root = ET.fromstring(raw_text)
            
            # API 에러 코드 체크
            result_code = root.find(".//resultCode")
            if result_code is not None and result_code.text != "00":
                msg = root.find(".//resultMsg").text if root.find(".//resultMsg") is not None else "Unknown"
                logger.error(f"❌ API 로직 에러 ({result_code.text}): {msg}")
                return None

            items = []
            for item_node in root.findall(".//item"):
                items.append({child.tag: child.text for child in item_node})
            return items

        logger.error(f"❌ 알 수 없는 응답 구조 (앞부분): {raw_text[:50]}")
        return None

    except Exception as e:
        logger.error(f"🚨 API 호출 과정 중 예외 발생: {e}")
        return None

def main():
    logger.info("🚀 데이터 수집 파이프라인 최종 가동")
    producer = get_kafka_producer()
    if not producer: return

    for region in TARGET_REGION_CODES:
        logger.info(f"📍 지역 코드 [{region}] 데이터 처리 중...")
        items = fetch_page(region, 1)
        
        if not items:
            logger.warning(f"⚠️ [{region}] 수집된 항목이 없습니다.")
            continue

        success_count = 0
        for item in items:
            try:
                item['collected_at'] = datetime.now().isoformat()
                item['region_code'] = region
                item['source'] = 'SBiz_API'
                
                producer.send(KAFKA_TOPIC, value=item)
                success_count += 1
            except Exception as e:
                logger.error(f"❌ Kafka 전송 실패: {e}")

        producer.flush()
        logger.info(f"✅ [{region}] 수집 및 전송 완료: {success_count}건")
        time.sleep(1.0)

    producer.close()
    logger.info("🏁 수집 프로세스 종료")

if __name__ == "__main__":
    main()