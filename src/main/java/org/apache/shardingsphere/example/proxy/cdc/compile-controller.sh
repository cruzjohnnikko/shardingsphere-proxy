#!/bin/bash
# Script to compile only the updated controller

cd /Users/johnnikko.cruz/Desktop/Playground/shardingsphere-proxy-cdc

# Compile just the controller with classpath from Maven
javac -cp "$(mvn dependency:build-classpath | grep -v '\[INFO\]'):target/classes" \
  -d target/classes \
  src/main/java/org/apache/shardingsphere/example/proxy/cdc/controller/CDCController.java

# Copy frontend files
cp src/main/resources/static/js/app.js target/classes/static/js/app.js 2>/dev/null
cp src/main/resources/static/index.html target/classes/static/index.html 2>/dev/null

echo "Controller compiled and frontend files copied successfully!"

