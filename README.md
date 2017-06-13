# JBlosc2

| **Travis CI** | **Appveyor** |
|---------------|--------------|
|[![Build Status](https://travis-ci.org/Blosc/JBlosc2.svg?branch=master)](https://travis-ci.org/Blosc/JBlosc2) |[![Build status](https://ci.appveyor.com/api/projects/status/k7ler2n0ytmbjrw6?svg=true)](https://ci.appveyor.com/project/FrancescAlted/jblosc2)|

Java interface for Blosc2

The purpose of this project is to create a Java interface for the compressor Blosc. JNA has been chosen as the mechanism to communicate with the Blosc2 shared library.

A simple example extracted from the unit tests:
```java
    int SIZE = 100 * 100 * 100;
    ByteBuffer ibb = ByteBuffer.allocateDirect(SIZE * PrimitiveSizes.DOUBLE_FIELD_SIZE);
    for (int i = 0; i < SIZE; i++) {
        ibb.putDouble(i);
    }
    JBlosc2 jb2 = new JBlosc2();
    ByteBuffer obb = ByteBuffer.allocateDirect(ibb.limit() + JBlosc2.OVERHEAD);
    jb2.compress(5, Shuffle.BYTE_SHUFFLE, PrimitiveSizes.DOUBLE_FIELD_SIZE, ibb, ibb.limit(), obb, obb.limit());
    ByteBuffer abb = ByteBuffer.allocateDirect(ibb.limit());
    jb2.decompress(obb, abb, abb.limit());
    jb2.destroy();
    assertEquals(ibb, abb);
```
## Installation
First of all, you need to install the Blosc2 library (visit https://github.com/Blosc/c-blosc2 for more details), in short, if you already have CMake, executing the following commands should do the work:
```bash
git clone https://github.com/Blosc/c-blosc2.git
cd c-blosc2
mkdir build
cd build
cmake -DCMAKE_GENERATOR_PLATFORM=x64 ..
cmake --build . --target install
```
Tipically in Linux/Unix the Blosc2 library is installed in your system search path, however, in Windows you will need to add blosc.dll to your PATH (```copy "c:\Program Files (x86)\blosc\lib\blosc.dll" c:\Windows\System32```).

Also check that your OS, Java Virtual Machine and Blosc library are using the same architecture (either 32 or 64 bit).

Build: ```mvn clean install```

If you want to use it in another maven project, after installing it you can use it as a dependency like this:
```xml
		<dependencies>
			<dependency>
				<groupId>org.blosc</groupId>
				<artifactId>jblosc2</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
		</dependencies>
```
