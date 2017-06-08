package org.blosc.examples;

import java.nio.ByteBuffer;

import org.blosc.JBlosc2;
import org.blosc.PrimitiveSizes;
import org.blosc.Util;
import org.blosc.jna.BloscLibrary;
import org.blosc.jna.Sheader;
import org.blosc.jna.Sparams;

public class JBlosc2Invoker {

	public static void main(String[] args) {
		int SIZE = 100 * 100 * 100;
		double data[] = new double[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = i;
		}
		ByteBuffer ibb = Util.array2ByteBuffer(data);
		JBlosc2 jb2 = new JBlosc2();
		jb2.init();
		int iBufferSize = SIZE * PrimitiveSizes.DOUBLE_FIELD_SIZE;
		int oBufferSize = SIZE * PrimitiveSizes.DOUBLE_FIELD_SIZE + JBlosc2.OVERHEAD;
		ByteBuffer obb = ByteBuffer.allocateDirect(oBufferSize);
		int w = jb2.compress(5, 1, PrimitiveSizes.DOUBLE_FIELD_SIZE, ibb, iBufferSize, obb, oBufferSize);

		Sparams sparams = new Sparams();
		sparams.filters[0] = BloscLibrary.BLOSC_DELTA;
		sparams.filters[1] = BloscLibrary.BLOSC_SHUFFLE;
		Sheader sheader = jb2.newSchunk(sparams);
		/* Now append a couple of chunks */
		int nchunks = jb2.appendBuffer(sheader, PrimitiveSizes.DOUBLE_FIELD_SIZE, iBufferSize, ibb);
		assert (nchunks == 1);
		nchunks = jb2.appendBuffer(sheader, PrimitiveSizes.DOUBLE_FIELD_SIZE, iBufferSize, ibb);
		assert (nchunks == 2);

		/* Retrieve and decompress the chunks (0-based count) */
		int dsize = jb2.decompressChunk(sheader, 0, obb, iBufferSize);
		if (dsize < 0) {
			System.out.println("Decompression error.  Error code: " + dsize);
			return;
		}
		dsize = jb2.decompressChunk(sheader, 1, obb, iBufferSize);
		if (dsize < 0) {
			System.out.println("Decompression error.  Error code: " + dsize);
			return;
		}

		System.out.println("Decompression succesful!");

		for (int i = 0; i < SIZE; i++) {
			if (ibb.getDouble(i) != obb.getDouble(i)) {
				// printf("i, values: %d, %f, %f\n", i, data[i], data_dest[i]);
				System.out.println("Values " + ibb.getDouble(i) + ", " + obb.getDouble(i));
				System.out.println("Decompressed data differs from original at " + i + "!");
				return;
			}
		}

		System.out.println("Succesful roundtrip!");

		ByteBuffer abb = ByteBuffer.allocateDirect(iBufferSize);
		jb2.decompress(obb, abb, iBufferSize);

		jb2.destroySchunk(sheader);

		double[] data_again = Util.byteBufferToDoubleArray(abb);
		jb2.destroy();
		System.out.println(
				"Items Original " + data.length + ", Items compressed " + w + ", Items again " + data_again.length);
		System.out.println("Finished!");
	}
}
