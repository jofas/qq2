from confluent_kafka import Producer

msg = \
b"""{
"service_name": "1_GoKit_2",
"operation": "Test",
"message": "Test"
}"""

print(msg)

p = Producer({'bootstrap.servers':'qq2.ddnss.de:9092'})

def cb(err, msg):
    if err is not None:
        print("ERROR: {}".format(err))
    else:
        print("SUCCESS: {}".format(msg))

p.poll(0)

p.produce('logging', msg, callback=cb)

p.flush()
