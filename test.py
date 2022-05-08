import os
from kafka import KafkaProducer
import time

for i in range(10000000):
	print("printing")
	producer = KafkaProducer(bootstrap_servers=['localhost:29092'])
	callbk = producer.send("tweets", str(i).encode('utf-8'))

	# Block for 'synchronous' sends
	try:
	    record_metadata = callbk.get(timeout=5)
	except KafkaError:
	    # Decide what to do if produce request failed...
	    print("Failed!")
	    log.exception()
	    pass
	time.sleep(5)

	print("sent successfully")