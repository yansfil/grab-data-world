# Simple ETL 파이프라인 (Beta)
Local Machine에서 Container를 바탕으로 ETL 파이프라인을 구축하였습니다. 

## 시작하기
전부 실행시키기

    $ docker-compose up

백그라운드 모드로 실행시키기

    $ docker-compose up -d



## 스택
### Scheduler 
Airflow
- localhost:9000

  
### Data Source 
Postgresql
- localhost:5432

### Visualization & REPL
Jupyter
- localhost:8888

### Data Ingestion
Spark(Local mode) & Python 







