lambda_achitecture
==============================================

Copyright (c) 2020 Vu The Anh.

[https://github.com/theanhvu1998/lambda_achitecture](https://github.com/theanhvu1998/lambda_achitecture)

# Overview

# Reference Docs
* kafka [here](https://kafka.apache.org/documentation/)
* Lambda architecture [here](http://itechseeker.com/projects/implement-lambda-architecture-2/)
* kafka connector [here](https://docs.lenses.io/)

# Prerequisites
* Ubuntu 18.04
* Ram 4G
* Disk 80G(SSD)

# Requirements

* Apache Kafka =2.1.0
* Node.js >=4
* Linux/Mac
* cassandra >= 3.10.x
* Scala 2.11.x
* spark 2.3.0

# Install

## Kafka
### Step 1: Download Apache Kafka

Now let’s download Kafka, you can go to [here](https://kafka.apache.org/downloads) and download the latest release if necessary. The latest download link at the time of writing has already been entered in the example for you(replace 2.2.0 by your version).

```
$ wget https://www-us.apache.org/dist/kafka/2.2.0/kafka_2.12-2.2.0.tgz -O kafka.tgz
```
### Step 2: Extract Apache Kafka
Now that the Apache Kafka binary has been downloaded, now we need to extract it in our Kafka user directory

```
$ tar -xzvf kafka.tgz --strip
```

### Step 3: Test Apache Kafka
Zookeeper is required for running Kafka
#### Runing Zookeeper

Opend terminal

```
$ /home/anhvu/kafka/bin/zookeeper-server-start.sh /home/anhvu/kafka/config/zookeeper.properties
```

#### Runing kafka

Opend other terminal(keep Zookeeper's terminal)

```
$ /home/anhvu/kafka/bin/kafka-server-start.sh /home/anhvu/kafka/config/server.properties > /home/anhvu/kafka/kafka.log 2>&1
```

#### Create topic

Opend other terminal

```
$ /home/anhvu/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```

#### Check list topic

Opend other terminal

```
$ /home/anhvu/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
```

#### Lissten topic

Opend other terminal

```
$ /home/anhvu/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

#### Send messen to topic

Opend other terminal

```
$ /home/anhvu/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```

That’s it! Apache Kafka has been successfully installed and set up. Now we can type some messages on the producer terminal as stated in the previous step. The messages will be immediately visible on our consumer terminal.

## MQTT
### Step 1: Install MQTT

Now, install Mosquitto using apt install:

```
$ sudo apt install mosquitto mosquitto-clients
```

### Step 2: Configuring MQTT Passwords

Let’s configure Mosquitto to use passwords. Mosquitto includes a utility to generate a special password file called mosquitto_passwd. This command will prompt you to enter a password for the specified username, and place the results in /etc/mosquitto/passwd:

```
$ sudo mosquitto_passwd -c /etc/mosquitto/passwd user_name
```

Now we’ll open up a new configuration file for Mosquitto and tell it to use this password file to require logins for all connections:

```
$ sudo nano /etc/mosquitto/conf.d/default.conf
```
This should open an empty file. Paste in the following:

```
allow_anonymous false
password_file /etc/mosquitto/passwd
```
Be sure to leave a trailing newline at the end of the file.

Now we need to restart Mosquitto and test our changes.

```
$ sudo systemctl restart mosquitto
```

### Step 2: Test

#### Subscribe topic

```
$ mosquitto_sub -h localhost -t test -u "sammy" -P "password"
```

#### Publish messen to topic

```
$ mosquitto_pub -h localhost -t "test" -m "hello world" -u "sammy" -P "password"
```

# Configuration

##  Config kafka

###  created service
#### zookeeper service
```
$ sudo nano /etc/systemd/system/zookeeper.service
```
Paste the following lines into it:
```
[Unit]
Requires=network.target remote-fs.target
After=network.target remote-fs.target
[Service]
Type=simple
ExecStart=/home/anhvu/kafka/bin/zookeeper-server-start.sh /home/anhvu/kafka/config/zookeeper.properties
ExecStop=/home/anhvu/kafka/bin/zookeeper-server-stop.sh
Restart=on-abnormal
[Install]
WantedBy=multi-user.target
```

#### kafka service
```ubuntu
$ sudo nano /etc/systemd/system/kafka.service
```
Paste the following lines into it:
```
[Unit]
Requires=zookeeper.service
After=zookeeper.service
[Service]
Type=simple
ExecStart=/bin/sh -c '/home/anhvu/kafka/bin/kafka-server-start.sh /home/anhvu/kafka/config/server.properties > /home/anhvu/kafka/kafka.log 2>&1'
ExecStop=/home/anhvu/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal
[Install]
WantedBy=multi-user.target
```

#### lambda service
```ubuntu
$ sudo nano /etc/systemd/system/lambda.service
```
Paste the following lines into it:
```
[Unit]
Requires=kafka.service
After=kafka.service
[Service]
Type=simple
ExecStart=/home/anhvu/kafka/bin/connect-standalone.sh /home/anhvu/kafka/config/connect-standalone.properties /home/anhvu/kafka/config/cassandra-sink-lambda.properties /home/anhvu/kafka/config/mqtt-source-lambda.properties
Restart=on-abnormal
[Install]
WantedBy=multi-user.target
```

### run service

```
$ sudo systemctl enable lambda
$ sudo systemctl start lambda
```

# Usage
## Run jar file

Replace /home/anhvu/lambda_achitecture/code/dist/scala/lambda_jarlambda.jar by path to your jar file and main_package.main by your class name

```
$ spark-submit --class main_package.main --master local /home/anhvu/lambda_achitecture/code/dist/scala/lambda_jar/lambda.jar
```