#!/bin/sh

java -cp $(echo lib/*.jar | tr ' ' ':'):bin Main 127.0.0.1:60010
