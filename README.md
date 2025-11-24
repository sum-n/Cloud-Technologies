# Commands:
## To check if Docker is running
```https
docker --version
```
## To check if Docker Compose is running
```https
docker-compose --version
```
## To check if there are any containers already running
```https
docker ps
```
## To remove old containers if there are already running containers
```https
docker container prune -f
```
## To build the image
```https
docker-compose build
```
## To start fresh
```https
docker-compose up -d
```
## To start normally
```https
docker-compose up
```
## To wait few seconds for services to start
```https
sleep 10
```
## To check if containers are up
```https
docker ps
```
## Starting Spark Session with main.py script (only show errors)
```https
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /app/main.py 2>&1 | grep -v "INFO"
```
## Stop the docker 
```https
docker-compose down
```
