#!/usr/bin/bash

# source-debezium
cd source-debezium || exit
./gradlew buildImages

# sink-jdbc
cd ../sink-jdbc || exit
./gradlew buildImages