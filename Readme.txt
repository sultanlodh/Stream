1. Go to your working directory
2. Build docker image
3. Then run docker container
docker build -t linearizestream:latest .
docker run -it -d --name LinearizeStream --network debezium_default linearizestream:latest