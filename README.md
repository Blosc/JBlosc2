# jblosc2
Java interface for Blosc2

The purpose of this project is to create a Java interface for the compressor Blosc. JNA has been chosen as the mechanism to communicate with the Blosc2 shared library.

A simple example extracted from the unit tests:

		int SIZE = 100 * 100 * 100;
		ByteBuffer ibb = ByteBuffer.allocateDirect(SIZE * PrimitiveSizes.DOUBLE_FIELD_SIZE);
		for (int i = 0; i < SIZE; i++) {
			ibb.putDouble(i);
		}
		BloscWrapper bw = new BloscWrapper();
		bw.init();
		ByteBuffer obb = ByteBuffer.allocateDirect(ibb.limit() + BloscWrapper.OVERHEAD);
		bw.compress(5, Shuffle.BYTE_SHUFFLE, PrimitiveSizes.DOUBLE_FIELD_SIZE, ibb, ibb.limit(), obb, obb.limit());
		ByteBuffer abb = ByteBuffer.allocateDirect(ibb.limit());
		bw.decompress(obb, abb, abb.limit());
		bw.destroy();
		assertEquals(ibb, abb);

Blosc2 shared library should be in PATH on Windows or in LD_LIBRARY_PATH on Linux/Unix.

Also check that your OS, Java Virtual Machine and blosc.dll are using the same architecture (either 32 or 64 bit).  
In case you are using Windows with Microsoft Visual Studio compiler and you need to enforce the 64 bits architecture for blosc.dll, you can do this by adding in cmake the flag `-A x64` (e.g. `cmake -A x64 -DPREFER_EXTERNAL_ZLIB=OFF ..`).

Build: mvn clean install

If you want to use it in another maven project, after installing it you can use it as a dependency like this:

		<dependencies>
			<dependency>
				<groupId>com.jblosc</groupId>
				<artifactId>jblosc2</artifactId>
				<version>0.0.1-SNAPSHOT</version>
			</dependency>
		</dependencies>
