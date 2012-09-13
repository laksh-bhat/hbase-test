#!/bin/sh

javac -cp $(echo lib/*.jar | tr ' ' ':') -d bin src/Main.java
javac -cp $(echo lib/*.jar | tr ' ' ':') -d bin src/Read.java
javac -cp $(echo lib/*.jar | tr ' ' ':') -d bin src/Write.java
