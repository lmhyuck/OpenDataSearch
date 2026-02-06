import json
import time
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

# Kafka 생산자 설정
producer = KafkaProducer(
    bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS')],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 0),
    acks=1 # 1명(리더)만 확인하면 바로 다음 데이터 전송! (속도 향상)
)

def collect_massive_data():
    """
    실제 공공데이터 수백 개를 생성하거나 파일을 읽어 Kafka로 쏘는 함수입니다.
    """
    print("📂 대량 데이터 수집 프로세스를 시작합니다...")
    
    # 1. 테스트를 위한 대량의 가공 데이터 생성 (실제로는 CSV나 API 결과물)
    # 100개의 서로 다른 공공데이터 메타데이터를 순식간에 만들어낼게요.
    bulk_data = []
    categories = ["교통", "환경", "안전", "교육", "보건", "문화"]
    orgs = ["서울시", "경기도", "부산시", "환경부", "행정안전부"]

    for i in range(1, 101): # 100개의 데이터를 생성합니다.
        item = {
            "id": f"data-{i:03d}",
            "title": f"2026년 {orgs[i % 5]} {categories[i % 6]} 지표 데이터 {i}",
            "desc": f"본 데이터는 {orgs[i % 5]}에서 제공하는 {categories[i % 6]} 관련 상세 수치 정보입니다.",
            "category": categories[i % 6],
            "org": orgs[i % 5],
            "collected_at": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        bulk_data.append(item)

    # 2. 데이터를 하나씩 Kafka로 전송
    count = 0
    for data in bulk_data:
        try:
            # 영수증(future)을 받지만, 속도를 위해 100개마다만 확인해볼까요?
            producer.send('public-metadata', value=data)
            count += 1
            if count % 20 == 0:
                print(f"📦 현재 {count}개 데이터 전송 중...")
        except Exception as e:
            print(f"❌ 전송 실패: {e}")

    # 모든 데이터가 확실히 나갈 때까지 잠시 기다려줍니다 (중요!)
    producer.flush() 
    print(f"✅ 총 {count}개의 데이터가 성공적으로 Kafka 고속도로에 올라탔습니다!")

if __name__ == "__main__":
    start_time = time.time()
    collect_massive_data()
    print(f"⏱ 소요 시간: {time.time() - start_time:.2f}초")