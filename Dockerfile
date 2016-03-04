FROM repo.docker.zplay.cc/node-dev:latest
ADD . /data/projects/redis-test/
WORKDIR /data/projects/redis-test/examples/
RUN npm install -d
