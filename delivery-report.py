def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")