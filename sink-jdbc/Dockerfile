FROM openjdk:18
RUN microdnf install findutils
COPY ./app/build/install/app /usr/src/app
WORKDIR /usr/src/app
ENTRYPOINT ["./bin/app"]
