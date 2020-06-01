import paho.mqtt.client as mqtt
import threading
import time
import json
import random
import math

# Define event callbacks
def on_connect(client, userdata, flags, rc):
    if rc==0:
        print("connected is OK")
    else:
        print("Bad connected wiht error :" + rc)
    
def on_message(client, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

def on_publish(client, obj, mid):
    print("mid: " + str(mid))

def on_subscribe(client, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_log(client, obj, level, string):
    print(string)

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print ("Unexpected MQTT disconnection. Will auto-reconnect")

# publish topic
def PublishTopic(mqttc,rc,topicm, deviceid):
    time.sleep(2)
    while rc==0:
        ran=random.randint(120,17280)
        timeo1=int(time.time()*1000)
        sessionid=deviceid+"-"+str(timeo1)+"-"+"r"
        tf=random.uniform(0.5, 2)
        ttw=random.randint(120, 300)
        tw0=random.randint(15, 80)

        mssido1=deviceid+"-"+str(timeo1)+"-"+"operation"
        operation1={
            'rpi':deviceid,
            'time': timeo1,
            'type': 'operation',
            'msgid': mssido1,
            'sessionid': sessionid,
            'data':[
                {"id":"g01opetb","v":str(timeo1)},
                {"id":"g01opete","v":str(0)}
            ]
            }
        messeneoa=json.dumps(operation1)
        mqttc.publish(topicm, messeneoa)
        for i in range(ran):
            time.sleep(5)
            errort=random.uniform(-1, 1)
            g01eleia=random.uniform(40, 55)
            g01eleib=random.uniform(50, 60)
            g01eleic=random.uniform(40, 55)
            g01elevab=random.uniform(240, 380)
            g01eles=random.uniform(90, 113)
            f=random.randint(1480, 1520)

            timee=int(time.time()*1000)

            msside=deviceid+"-"+str(timee)+"-"+"electrical"
            te=i+errort
            g01elef=f*(1-math.e**(-te/tf))
            electrical={
            'rpi':deviceid,
            'time': timee,
            'type': 'electrical',
            'msgid': msside,
            'sessionid': sessionid,
            'data':[
                {"id":"g01eleia","v":str(g01eleia)},
                {"id":"g01eleib","v":str(g01eleib)},
                {"id":"g01eleic","v":str(g01eleic)},
                {"id":"g01elevab","v":str(g01elevab)},
                {"id":"g01eles","v":str(g01eles)},
                {"id":"g01elef","v":str(g01elef)}
            ]
            }

            messene=json.dumps(electrical)
            mqttc.publish(topicm, messene)
            time.sleep(3)

            erroro2=random.uniform(-0.4, 0.4)
            errorh2s=random.uniform(-10, 10)
            tw=random.randint(55, 65)

            timeev=int(time.time()*1000)

            mssidev=deviceid+"-"+str(timeev)+"-"+"environmental"

            g01envtw=tw*(1-math.e**(-te/ttw))+tw0
            g01envpo=random.randint(1, 2)%2
            g01envo2=0.5+erroro2
            g01envh2s=80+errorh2s
            environmental={
            'rpi':deviceid,
            'time': timeev,
            'type': 'environmental',
            'msgid': mssidev,
            'sessionid': sessionid,
            'data':[
                {"id":"g01envtw","v":str(g01envtw)},
                {"id":"g01envpo","v":str(g01envpo)},
                {"id":"g01envo2","v":str(g01envo2)},
                {"id":"g01envh2s","v":str(g01envh2s)}
            ]
            }

            messenev=json.dumps(environmental)
            mqttc.publish(topicm, messenev)
            time.sleep(2)

        timeo2=int(time.time()*1000)
        mssido2=deviceid+"-"+str(timeo2)+"-"+"operation"
        operation2={
            'rpi':deviceid,
            'time': timeo2,
            'type': 'operation',
            'msgid': mssido2,
            'sessionid': sessionid,
            'data':[
                {"id":"g01opetb","v":str(timeo1)},
                {"id":"g01opete","v":str(timeo2)}
            ]
            }
        messeneob=json.dumps(operation2)
        mqttc.publish(topicm, messeneob)
    print("error")

# id of raspery
device_id = "go1"

# infor of mqtt broker
broker_url = 'localhost'
broker_port = 1883
broker_user_name = 'biogas'
broker_password = 'biogas'
topicm = 'messenger'
topics = 'status'

connect_string = 'connecting to broker ....'
Exception_string = "An exception occurred: \n ConnectionRefusedError \n check your mqtt broker"

mqttc = mqtt.Client()
# Assign event callbacks
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe
mqttc.on_message = on_message
mqttc.on_disconnect = on_disconnect
# mqttc.on_log = on_log
# Uncomment to enable debug messages

print(connect_string)
# Connect
try:
    timestt=int(time.time()*1000)
    mssidstt=device_id+"-"+str(timestt)+"-"+"status"
    mydata={
    'rpi': device_id,
    'type': 'status',
    'msgid': mssidstt,
    'status': 'offline'
    }
    strdata=json.dumps(mydata)
    mqttc.will_set(topics, payload=strdata, qos=2, retain=True)
    mqttc.username_pw_set(broker_user_name, broker_password)
    mqttc.connect(broker_url, broker_port)
    # Start subscribe, with QoS level 2
    mqttc.subscribe(topicm, 2)

    # Publish a message
    # mqttc.publish(topic, "my message")
    
    # Continue the network loop, exit when an error occurs
    rc = 0
    t1 = threading.Thread(target=PublishTopic, args=(mqttc,rc,topicm,device_id))
    t1.setDaemon(True)
    t1.start()
    mqttc.loop_forever()
except(KeyboardInterrupt, SystemExit, ConnectionRefusedError):
    print(Exception_string)
