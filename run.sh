#!/bin/bash

javac -sourcepath src -d bin src/**/**/**/*.java
java -cp bin: com.iiit.db.Driver $@

