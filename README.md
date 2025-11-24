Commands:
# To check if Docker is running
docker --version

# To check if Docker Compose is running
docker-compose --version

# To check if there are any containers already running
docker ps

# To remove old containers if there are already running containers
docker container prune -f

# To build the image
docker-compose build

# To start fresh
docker-compose up -d

# To start normally
docker-compose up

# To wait few seconds for services to start
sleep 10

# To check if containers are up
docker ps

# Starting Spark Session with main.py script (only show errors)
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /app/main.py 2>&1 | grep -v "INFO"

# Stop the docker 
docker-compose down