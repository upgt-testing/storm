# Build Instructions for Storm Restart Testing Adapter

## Overview

This is a **standalone Maven project** that integrates Apache Storm with the RestartTestingFramework.
It does not participate in Storm's multi-module build and can be built independently.

## Prerequisites

1. **Java 17 or higher** - Storm 2.8.4+ requires Java 17+
2. **Maven 3.6+**
3. **Storm project built** - `storm-server:2.8.4-SNAPSHOT` must be available in your local Maven repository
4. **RestartTestingFramework** - `restart-core:1.0.0-SNAPSHOT` must be available

## Building Dependencies First

Before building this adapter, you need to build and install its dependencies to your local Maven repository:

### 1. Build Storm

```bash
# Make sure you have Java 17+
java -version  # Should show 17 or higher

# Build Storm (from the storm root directory)
cd /home/shuai/xlab/restart_testing/storm
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
MAVEN_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.desktop/java.awt.font=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED" \
mvn clean install -DskipTests

# This will install storm-server and other dependencies to your local Maven repository
```

### 2. Build RestartTestingFramework

```bash
cd /home/shuai/xlab/restart_testing/RestartTestingFramework
mvn clean install -DskipTests
```

## Building the Adapter (Standalone)

Once dependencies are available in your local Maven repository:

```bash
cd /home/shuai/xlab/restart_testing/storm/restart-storm-adapter
mvn clean install
```

**Note**: This project is standalone and does NOT require `-pl` or reactor build from Storm's parent POM.

## Current Build Status

**Status**: Cannot build on this system
**Reason**: Java 8 is installed, but Storm 2.8.4 requires Java 17+

Available Java versions on this system:
- Java 8 (current): `/usr/lib/jvm/java-8-openjdk-amd64`
- Java 11: `/usr/lib/jvm/java-11-openjdk-amd64`

### Solution

Install Java 17 or higher:

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install openjdk-17-jdk

# Set Java 17 as default
sudo update-alternatives --config java
# Select Java 17 from the list

# Verify
java -version  # Should show version 17 or higher
```

## Alternative: Standalone Demo

For demonstration purposes without Java 17, see the standalone demo in `src/test/java/org/apache/storm/restart/StandaloneDemo.java` which shows the adapter structure without requiring the full Storm build.

## Testing

Once built:

```bash
# Run all tests
mvn test

# Run specific test
mvn test -Dtest=StormClusterAdapterTest#testSupervisorGracefulRestart
```

## Integration with RestartTestingFramework

This adapter assumes the RestartTestingFramework is available. If you're building it separately:

```bash
# Build RestartTestingFramework first
cd /home/shuai/xlab/restart_testing/RestartTestingFramework
mvn clean install -DskipTests

# Then build this adapter
cd /home/shuai/xlab/restart_testing/storm/restart-storm-adapter
mvn clean install
```

## Troubleshooting

### Error: "Could not find artifact org.apache.storm:storm-server"

**Solution**: Build Storm first with `mvn install` from the storm root directory.

### Error: "Detected JDK version... is not in the allowed range [17,)"

**Solution**: Upgrade to Java 17+. Storm 2.8.4-SNAPSHOT requires Java 17 or higher.

### Error: "Could not find artifact org.restarttest:restart-core"

**Solution**: Build the RestartTestingFramework project first.
