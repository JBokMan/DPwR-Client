# DPwR-Client

The Distributed Plasma with RDMA (DPwR) Client application is able to connect to one or more [DPwR-Server](https://github.com/JBokMan/DPwR-Server) applications to use the [Apache Plasma](https://github.com/apache/arrow/blob/master/cpp/apidoc/tutorials/plasma.md) store distributed. 
The PUT and GET operations utilize Remote Direct Memory Access (RDMA) through the use of [Infinileap](https://github.com/hhu-bsinfo/infinileap).
This repository also includes a [Yahoo! Cloud Serving Benchmark](https://github.com/brianfrankcooper/YCSB) (YCSB) binding for the benchmarking of DPwR.
The code in this project is experimental and written as part of a master thesis.

## Installation:

### Clone Submodules
After cloning the repository, the submodules must also be cloned, for example by using:

```git submodule update --init --recursive```

Add to infinileap: infinileap/core/src/main/java/de/hhu/bsinfo/infinileap/util/Requests.java
```java
public static void await(Worker worker, Queue queue) throws InterruptedException {
        while (queue.isEmpty()) {
            if (worker.progress() == WorkerProgress.IDLE) {
                worker.await();
            }

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }
```
And change infinileap: infinileap/core/src/main/java/de/hhu/bsinfo/infinileap/binding/MemoryRegion.java
```java
@Override
    public void close() throws Exception {
        var status = ucp_mem_unmap(context.address(), handle.address());
        if (Status.isNot(status, Status.OK)) {
            throw new CloseException(new ControlException(status));
        }
    }
```
To:
```java
@Override
    public void close() throws CloseException {
        var status = ucp_mem_unmap(context.address(), handle.address());
        if (Status.isNot(status, Status.OK)) {
            throw new CloseException(new ControlException(status));
        }
    }
```


### Install Java Panama
- Install sdk-man
1. ```curl -s "https://get.sdkman.io" | bash```
2. ```source "$HOME/.sdkman/bin/sdkman-init.sh"```
- Download panama installer from https://coconucos.cs.hhu.de/forschung/jdk/install and save it as panama-install.sh then run:
3. ```bash panama-install.sh```
4. ```sdk use java panama```

### Install UCX
Install UCX from https://github.com/openucx/ucx/releases/tag/v1.13.0

## How to run the client

1. run ```./gradlew installDist```
2. run ```export UCX_ERROR_SIGNALS=""```
3. run ```./build/install/InfinimumDB-Client/bin/InfinimumDB-Client```

## Known Bugs/Problems:

### Gradle File Not Found
Exception in thread "main" java.io.FileNotFoundException: https://downloads.gradle-dn.com/distributions-snapshots/gradle-7.5-20220113232546+0000-bin.zip

Solution
Download gradle nightly from https://gradle.org/nightly/

### Gradle unsupported class file major version
BUG! exception in phase 'semantic analysis' in source unit '_BuildScript_' Unsupported class file major version 63

Solution
```sdk install java 17.0.3.6.1-amzn```
```sdk use java 17.0.3.6.1-amzn```
For building, Java 17 is required, but for running, Java 19 is required, so after building, run ```sdk use java panama```

### LibLLVM not found
Exception in thread "main" java.lang.UnsatisfiedLinkError: /home/julian/.sdkman/candidates/java/panama/lib/libclang.so: libLLVM-11.so.1: cannot open shared object file: No such file or directory

Solution
```sudo apt-get install llvm-11```

### UCX not installed
fatal error: 'uct/api/uct.h' file not found

Solution
install UCX by following the [Installation](#installation).

### Linkage error class file versions
Error: LinkageError occurred while loading main class main.Application
java.lang.UnsupportedClassVersionError: main/Application has been compiled by a more recent version of the Java Runtime (class file version 63.0), this version of the Java Runtime only recognizes class file versions up to 61.0

Solution
```sdk use java panama```

### No stdc++ in java.library.path
Following error message appears when running the client ```java.lang.UnsatisfiedLinkError: no stdc++ in java.library.path:```

**Solution:**
The libstdc++.so is not found in the java.library.path. Run ```find / -name 'libstdc++.so*' 2>/dev/null``` to see if a libstdc++.so exists.
If there is one, but it is not named exactly libstdc++.so create a symlink ```ln -s /path/to/libstdc++.so.something /where/you/want/libtsdc++.so```.
Now add the symlink to the LD_LIBRARY_PATH environment variable, and when starting the client, add it to the library path with ```java -Djava.library.path=```.

If you did not find any libstdc++.so install the package ```libstdc++-10-dev``` via ```sudo apt-get install libstdc++-10-dev``` and you should find one.

