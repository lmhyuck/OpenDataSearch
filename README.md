# 🔎 OpenData-Search-Engine
> **공공데이터 메타데이터 수집 및 고속 검색 엔진 시스템**

공공데이터포털의 메타데이터를 **Kafka**를 통해 실시간으로 수집하고, **Elasticsearch**에 색인하여 **FastAPI** 기반의 웹 화면에서 초고속으로 검색할 수 있는 풀스택 데이터 파이프라인 프로젝트입니다.

---

## 🏗 시스템 아키텍처
본 프로젝트는 현대적인 데이터 처리 아키텍처인 **분산 메시지 큐**와 **전문 검색 엔진**을 결합한 구조를 가집니다.



1. **Data Collector**: 공공데이터 API 또는 배치 데이터를 수집하여 Kafka로 전송
2. **Apache Kafka**: 수집된 데이터를 안정적으로 중계하는 메시지 브로커
3. **Stream Processor**: Kafka에서 데이터를 소비(Consume)하여 Elasticsearch에 저장
4. **Elasticsearch**: 대량의 메타데이터를 저장하고 전문(Full-text) 검색 기능을 제공
5. **FastAPI (Backend)**: Elasticsearch와 통신하여 검색 API 서비스 (Port: 8000)
6. **Frontend**: 사용자 친화적인 검색 인터페이스 (HTML/CSS/JS)

---

## 🛠 사용 기술 스테택
- **Language**: Python 3.10+
- **Infrastructure**: Docker, Docker Compose
- **Message Broker**: Apache Kafka
- **Search Engine**: Elasticsearch 8.x
- **Backend Framework**: FastAPI, Uvicorn
- **Frontend**: Vanilla JS, CSS3, HTML5

---

🚀 실행 가이드 (Detailed Execution Guide)
먼저 백그라운드에서 데이터 고속도로(Kafka)와 창고(Elasticsearch)를 가동합니다.

Bash
# 프로젝트 루트 폴더에서 실행
docker-compose up -d
Tip: 실행 후 docker ps를 입력하여 elasticsearch, kafka, zookeeper 컨테이너가 정상적으로 Up 상태인지 확인하세요.

3. 서비스 실행 순서 (Service Startup Sequence)
각 서비스는 서로 연결되어 있으므로 아래 순서대로 터미널을 각각 열어서 실행해 주세요.

Step 1: 데이터 처리기 실행 (Stream Processor)
Kafka에서 데이터를 받아 Elasticsearch에 넣는 작업을 먼저 대기시켜야 합니다.

Step 2: API 서버 실행 (Backend)
웹 화면을 띄우고 검색 기능을 제공할 서버를 실행합니다.

Step 3: 데이터 수집기 실행 (Data Collector)
