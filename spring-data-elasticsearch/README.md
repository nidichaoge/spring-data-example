### 准备
##### 1. 使用docker拉取es镜像
    docker pull elasticsearch:7.5.1
##### 2. 启动一个es的docker容器
    docker run -it -d -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" --name elasticsearch_7.5.1 elasticsearch:7.5.1
##### 3. 通过如下命令确认启动成功
    curl 127.0.0.1:9200
##### 4. 运行kibana的docker容器并链接到elasticsearch
    docker run --link elasticsearch_7.5.1:elasticsearch -p 5601:5601 {docker-repo}:{version}
