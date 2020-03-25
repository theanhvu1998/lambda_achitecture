Config kafka
==============================================

# created service
## zookeeper service
```ubuntu
$ sudo nano /etc/systemd/system/zookeeper.service
```
Paste the following lines into it:
```
[Unit]
Requires=network.target remote-fs.target
After=network.target remote-fs.target
[Service]
Type=simple
ExecStart=/home/anhvu/kafka/bin/zookeeper-server-start.sh /home/anhvu/kafka/con$
ExecStop=/home/anhvu/kafka/bin/zookeeper-server-stop.sh
Restart=on-abnormal
[Install]
WantedBy=multi-user.target
```

## kafka service
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
ExecStart=/bin/sh -c '/home/anhvu/kafka/bin/kafka-server-start.sh /home/anhvu/k$
ExecStop=/home/anhvu/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal
[Install]
WantedBy=multi-user.target
```

## lambda service
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
ExecStart=/home/anhvu/kafka/bin/connect-standalone.sh /home/anhvu/kafka/config/$
ExecStop=/home/anhvu/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal
[Install]
WantedBy=multi-user.target
```

# run service

```
$ sudo systemctl enable kafka
$ sudo systemctl start kafka
$ sudo systemctl start lambda.service
```
