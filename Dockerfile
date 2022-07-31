FROM spark-base/spark:v0.0.1

USER root

WORKDIR /app

COPY ./src/main/resources/application.dev.properties .
COPY ./target/scala-2.12/telegram-job-processing.jar .

