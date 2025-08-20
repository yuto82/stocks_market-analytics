how to set-up poetry:
1. cd stocks_market-analytics
2. poetry init
3. poetry config virtualenvs.in-project true
4. mkdir src/market_analytics
5. touch src/market_analytics/__init__.py
6. poetry install
7. poetry env info --path
8. source .../.venv/bin/activate
9. deactivate



created docker-compose.yml
docker-compose up -d

docker exec -it $(docker ps -qf "ancestor=confluentinc/cp-kafka:7.5.0") \
  kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it $(docker ps -qf "ancestor=confluentinc/cp-kafka:7.5.0") \
  kafka-topics --delete --topic test-topic --bootstrap-server localhost:9092

  docker exec -it $(docker ps -qf "ancestor=confluentinc/cp-kafka:7.5.0") \
  kafka-topics --list --bootstrap-server localhost:9092

docker compose down --volumes --rmi all