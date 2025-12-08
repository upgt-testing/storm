# Standalone Project Status

## Overview

The Storm Restart Testing Adapter is now a **standalone Maven project** within the Storm repository. It does not participate in Storm's multi-module reactor build and can be built independently.

## Key Changes Made

### 1. POM Configuration
- **Removed**: `<parent>` reference to Storm's parent POM
- **Added**: Explicit `groupId`, `version`, and `packaging`
- **Added**: Properties section with all dependency versions
- **Added**: Explicit plugin versions and configuration
- **Result**: Completely self-contained Maven project

### 2. Project Coordinates

```xml
<groupId>org.apache.storm</groupId>
<artifactId>restart-storm-adapter</artifactId>
<version>1.0.0-SNAPSHOT</version>
<packaging>jar</packaging>
```

### 3. Dependencies

All dependencies now have explicit versions:
- `storm-server:2.8.4-SNAPSHOT` (from Storm project)
- `restart-core:1.0.0-SNAPSHOT` (from RestartTestingFramework)
- `slf4j-api:2.0.17`
- `junit-jupiter:5.11.4`

## Building the Standalone Project

### Prerequisites

The adapter requires its dependencies to be available in your local Maven repository:

1. **Build Storm**:
   ```bash
   cd /home/shuai/xlab/restart_testing/storm
   JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
   MAVEN_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED ..." \
   mvn clean install -DskipTests
   ```

2. **Build RestartTestingFramework**:
   ```bash
   cd /home/shuai/xlab/restart_testing/RestartTestingFramework
   mvn clean install -DskipTests
   ```

### Build the Adapter

Once dependencies are available:

```bash
cd /home/shuai/xlab/restart_testing/storm/restart-storm-adapter
mvn clean install
```

**No `-pl` flags needed!** This project builds independently.

## Advantages of Standalone Approach

1. **Independence**: No coupling to Storm's complex reactor build
2. **Simplicity**: Build only what you need when you need it
3. **Flexibility**: Can use different versions of Storm by changing property
4. **Portability**: Can be moved to any location, even outside Storm repository
5. **No Reactor Conflicts**: Doesn't interfere with Storm's multi-module build

## Integration with Storm Repository

While physically located in the Storm repository at:
```
/home/shuai/xlab/restart_testing/storm/restart-storm-adapter/
```

It is **NOT** listed in Storm's parent `pom.xml` `<modules>` section, making it completely independent.

## Version Management

To use a different Storm version, simply update the property in `pom.xml`:

```xml
<properties>
    <storm.version>2.8.4-SNAPSHOT</storm.version>  <!-- Change this -->
    ...
</properties>
```

## Files Modified

1. **pom.xml** - Converted to standalone (removed parent, added explicit versions)
2. **README.md** - Updated to clarify standalone nature
3. **README_BUILD.md** - Updated build instructions for standalone project
4. **STANDALONE_PROJECT.md** - This file (new)

## Verification

You can verify the standalone nature by running:

```bash
cd /home/shuai/xlab/restart_testing/storm/restart-storm-adapter
mvn help:effective-pom | grep -A 5 "artifactId"
```

Should show:
```xml
<groupId>org.apache.storm</groupId>
<artifactId>restart-storm-adapter</artifactId>
<version>1.0.0-SNAPSHOT</version>
```

With no parent POM inheritance.
