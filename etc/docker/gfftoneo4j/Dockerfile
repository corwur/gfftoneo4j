FROM openjdk:8-jdk-alpine
RUN apk update
RUN apk add bash
ENV NEO4J bolt://localhost
RUN mkdir /data
ADD gfftoneo4j-*.jar /gfftoneo4j.jar
ADD data/*.gff /data/
ADD demo.sh /
ADD wait-for-it.sh /
#ENTRYPOINT demo.sh