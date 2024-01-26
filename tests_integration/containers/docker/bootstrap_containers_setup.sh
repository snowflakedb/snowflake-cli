cd ..
docker build -t snowpark_test:1 containers/docker
snow registry token --format=json -c int | docker login
docker push snowpark_test:1
