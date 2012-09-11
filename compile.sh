#!/bin/sh

javac -cp $(echo lib/*.jar | tr ' ' ':') -d bin src/Main.java
