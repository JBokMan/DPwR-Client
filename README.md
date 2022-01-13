# InfinimumDB-Client

![Alt text](./documentation/images/Overview.svg)
![Alt text](./documentation/images/Initialization.svg)
![Alt text](./documentation/images/Put.svg)

## Problems & Solutions:

#### You get the error "java.lang.UnsatisfiedLinkError: no stdc++ in java.library.path"

1. locate your stdc++ files with ```whereis libstdc++```
2. copy all libstdc++ files to a folder within the *java.library.path* for example like
   this: ```sudo cp /usr/lib/x86_64-linux-gnu/11/libstdc++* /usr/lib64/```

#### In IntelliJ, you can not run the client and server examples at the same time:

1. Make sure you use Java 19, if not install it like
   this ```curl -s "https://coconucos.cs.hhu.de/forschung/jdk/install" | bash sdk use java panama```
2. Install the gradle [nightly](https://gradle.org/nightly/) build
3. In the IntelliJ *Settings*, set *Gradle JVM* to **Java 17**
4. In IntelliJ *Project Structure*, set *SDK* to **19** and *Language level* to **X - Experimental features** also set
   the *Module SDK* to **19**
