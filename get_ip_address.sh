#!/usr/bin/env bash
# Get the first non-loopback address of the machine

export IP_ADDRESS="$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | head -n 1)"
echo -n $IP_ADDRESS