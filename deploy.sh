#!/bin/bash

echo "=== Parking System Deploy ==="

TOMCAT_DIR="/home/raghu-pt8232/Desktop/apache-tomcat"
WAR_FILE="/home/raghu-pt8232/Desktop/vehicle_management/out/vehicle_management.war"

# Stop Tomcat
echo "Stopping Tomcat..."
$TOMCAT_DIR/bin/shutdown.sh 2>/dev/null
sleep 5

# Remove old deployment
echo "Removing old deployment..."
rm -rf $TOMCAT_DIR/webapps/vehicle_management
rm -f  $TOMCAT_DIR/webapps/vehicle_management.war

# Clear Tomcat work cache (THIS WAS MISSING - causes duplicate servlet error)
echo "Clearing Tomcat work cache..."
rm -rf $TOMCAT_DIR/work/Catalina/localhost/vehicle_management

# Copy new WAR
echo "Copying WAR..."
cp $WAR_FILE $TOMCAT_DIR/webapps/

# Start Tomcat
echo "Starting Tomcat..."
$TOMCAT_DIR/bin/startup.sh

echo ""
echo "=== Deploy Complete ==="
echo "Watch logs: tail -f $TOMCAT_DIR/logs/catalina.out"
echo "Test URL: http://localhost:8080/vehicle_management/login"