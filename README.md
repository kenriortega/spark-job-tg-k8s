# Telegram Jobs Offers Pipe line



### Deploy spark job on k8s


> Documentation way

Download [spark](https://spark.apache.org/downloads.html) from official site and follow this steps

Create spark base app

```bash
./bin/docker-image-tool.sh -r spark-base -t v0.0.1 build
```

create you .jar file if you use sbt (find how to use sbt-assembly to create a jar with all dependency that you need)

```bash
sbt clean
sbt assembly
```

Then create a docker image that contain to spark - job application an all your files

```dockerfile
FROM spark-base/spark:v0.0.1

USER root

WORKDIR /app

COPY ./src/main/resources/application.properties .
COPY ./target/scala-2.12/<name-jar>.jar .

```

Use spark submit command inside you spark folder or environment pc

```bash

../bin/spark-submit \
    --master k8s://http://127.0.0.1:8001 \
    --deploy-mode cluster \
    --name spark-telegram-jobs \
    --class TelegramJobProcessing \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image="spark-job-tg:v0.0.1" \
    local:///app/telegram-job-processing.jar
```

Other way is to use spark-operator

[https://github.com/GoogleCloudPlatform/spark-on-k8s-operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)


### Resources

[https://spark.apache.org/docs/latest/running-on-kubernetes.html](https://spark.apache.org/docs/latest/running-on-kubernetes.html)