import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

# 환경 설정 로드!
load_dotenv()

# Kafka에서 데이터를 꺼내올 '소비자(Consumer)'를 준비합니다.
consumer = KafkaConsumer(
    'public-metadata',  # 수집가가 데이터를 담았던 바로 그 바구니 이름!
    bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS')],
    group_id='metadata-group',     # 이 그룹끼리 협력해서 데이터를 처리해요.
    auto_offset_reset='earliest',  # 처음 온 데이터부터 하나도 놓치지 않고 다 읽을게요.
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    api_version=(0, 10, 0)
)

# 우리의 검색 엔진 본체, Elasticsearch에 연결합니다.
es = Elasticsearch(os.getenv('ELASTICSEARCH_URL'))

# 검색 엔진에 저장될 서랍 이름(Index)
INDEX_NAME = "public_metadata"

def create_index():
    """
    Elasticsearch에 데이터를 담을 서랍(Index)이 없으면 새로 만드는 함수예요.
    """
    if not es.indices.exists(index=INDEX_NAME):
        # 인덱스가 없을 때만 새로 생성!
        es.indices.create(index=INDEX_NAME)
        print(f"📦 검색 엔진에 '{INDEX_NAME}' 서랍을 새로 만들었습니다.")

if __name__ == "__main__":
    # 1. 먼저 서랍이 있는지 확인하고!
    create_index()
    
    print("🕵️ Kafka 바구니를 지켜보는 중... 데이터가 들어오면 바로 정리할게요!")

    # 2. Kafka 바구니에 데이터가 들어올 때까지 기다렸다가 하나씩 꺼냅니다.
    for message in consumer:
        metadata = message.value
        
        try:
            # Elasticsearch 서랍에 데이터를 집어넣습니다(Index).
            # id를 데이터의 고유번호로 지정해서 똑같은 데이터가 두 번 저장되지 않게 해요.
            res = es.index(index=INDEX_NAME, id=metadata['id'], document=metadata)
            
            # 정리가 잘 됐는지 확인 도장을 쾅! 찍어줍니다.
            print(f"📂 색인 성공: [{metadata['title']}] (결과: {res['result']})")
            
        except Exception as e:
            print(f"⚠️ 정리 중 에러 발생: {e}")