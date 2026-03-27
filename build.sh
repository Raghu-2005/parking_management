#!/bin/bash

echo "=== Parking System Build ==="

PROJECT_DIR="/home/raghu-pt8232/Desktop/vehicle_management"
SRC_DIR="$PROJECT_DIR/src"
OUT_DIR="$PROJECT_DIR/WebContent/WEB-INF/classes"
LIB_DIR="$PROJECT_DIR/WebContent/WEB-INF/lib"
WAR_NAME="vehicle_management.war"
OUT_WAR_DIR="$PROJECT_DIR/out"
SERVLET_JAR="jakarta.servlet-api-6.0.0.jar"

# Clean old classes and WAR
echo "Cleaning old classes..."
rm -rf "$OUT_DIR"/*
mkdir -p "$OUT_DIR"
mkdir -p "$OUT_WAR_DIR"

# Compile - explicitly list the jar for javac classpath
echo "Compiling Java files..."
find "$SRC_DIR" -name "*.java" > sources.txt

CLASSPATH=""
for jar in "$LIB_DIR"/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done
# Remove leading colon
CLASSPATH="${CLASSPATH:1}"

javac -cp "$CLASSPATH" \
      -d "$OUT_DIR" \
      --release 17 \
      @sources.txt

if [ $? -ne 0 ]; then
    echo "COMPILATION FAILED"
    exit 1
fi

echo "Compilation successful"

# Temporarily move servlet-api OUT before packaging
# (Tomcat 11 provides its own - including it causes conflicts)
echo "Excluding servlet-api from WAR (Tomcat provides it)..."
mv "$LIB_DIR/$SERVLET_JAR" "$PROJECT_DIR/$SERVLET_JAR"

# Create WAR
echo "Creating WAR file..."
cd "$PROJECT_DIR/WebContent"
jar cvf "$OUT_WAR_DIR/$WAR_NAME" .

# Restore servlet-api for future compilations
mv "$PROJECT_DIR/$SERVLET_JAR" "$LIB_DIR/$SERVLET_JAR"

echo "WAR created at: $OUT_WAR_DIR/$WAR_NAME"
echo "=== Build Complete ==="