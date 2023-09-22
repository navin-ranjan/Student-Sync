#!/bin/bash

echo "Starting script..."

# Change to the Spark directory (replace $SPARK_HOME with your actual Spark home directory)
cd $SPARK_HOME

# Start the Spark standalone master in the background
bin/spark-class org.apache.spark.deploy.master.Master &

# Sleep for a few seconds to allow the master to start (adjust the sleep duration as needed)
sleep 5

echo "Spark master standalone started ............."

# Define the myip function
myip() {
    hostname -I | awk '$1 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ {print $1}'
}

# Get the IP address using the myip function and store it in a variable
my_ip=$(myip)

# Start the first Spark worker in the background
bin/spark-class org.apache.spark.deploy.worker.Worker -c 2 -m 5G "spark://$my_ip:7077" &

echo "Spark worker1 added on Spark master standalone ............."

# Start the second Spark worker in the background
bin/spark-class org.apache.spark.deploy.worker.Worker -c 2 -m 5G "spark://$my_ip:7077" &

echo "Spark worker2 added on Spark master standalone ............."

echo "Calling myip function..."
myip

echo "Script finished."

