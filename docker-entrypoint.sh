#!/bin/ash
set -e

user="$(id -u)"
if [ "$user" = '0' ]; then
	[ -d "/mosquitto" ] && chown -R mosquitto:mosquitto /mosquitto || true
fi

if [ ! -e /mosquitto/config/mosquitto.conf ]; then
    if [ "$MOSQUITTO_PORT" == "" ]; then
      MOSQUITTO_PORT=1883
    fi

    echo 'allow_anonymous true
    listener '$MOSQUITTO_PORT'
    persistence true' > /mosquitto/config/mosquitto.conf
fi

/usr/sbin/mosquitto -c /mosquitto/config/mosquitto.conf -d

echo "Waiting for mosquitto..."
count=1
until mosquitto_pub -t fed/wait -m wait &> /dev/null; do
    sleep 2
    count=`expr $count + 1`
    if [ "$count" -eq 120 ]; then
        break
    fi
done
echo "Mosquitto ready!"

exec "mqtt-fed"