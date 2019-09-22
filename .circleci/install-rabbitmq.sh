#!/bin/bash


set -euxo pipefail

if [[ $(id -u) -ne 0 ]]; then
  echo 1>&2 "Must be run as root"
  exit 1
fi

apt-get update
apt-get install -y rabbitmq-server netcat
rabbitmq-plugins enable rabbitmq_stomp
/etc/init.d/rabbitmq-server start

for _ in $(seq 10); do 
  if nc -q 0 localhost 61613 -v < /dev/null; then
    break
  fi
done

nc -q 0 localhost 61613 -v < /dev/null
