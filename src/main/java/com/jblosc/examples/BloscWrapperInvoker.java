package com.jblosc.examples;

import java.nio.ByteBuffer;

import com.jblosc.BloscWrapper;
import com.jblosc.PrimitiveSizes;
import com.jblosc.Util;
import com.jblosc.jna.BloscLibrary;
import com.jblosc.jna.Sheader;
import com.jblosc.jna.Sparams;

public class BloscWrapperInvoker {

	public static void main(String[] args) {
		int SIZE = 100 * 100 * 100;
		double data[] = new double[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = i;
		}
		ByteBuffer ibb = Util.array2ByteBuffer(data);
		BloscWrapper bw = new BloscWrapper();
		bw.init();
		int iBufferSize = SIZE * PrimitiveSizes.DOUBLE_FIELD_SIZE;
		int oBufferSize = SIZE * PrimitiveSizes.DOUBLE_FIELD_SIZE + BloscWrapper.OVERHEAD;
		ByteBuffer obb = ByteBuffer.allocateDirect(oBufferSize);
		int w = bw.compress(5, 1, PrimitiveSizes.DOUBLE_FIELD_SIZE, ibb, iBufferSize, obb, oBufferSize);

		Sparams sparams = new Sparams();
		sparams.filters[0] = BloscLibrary.BLOSC_DELTA;
		sparams.filters[1] = BloscLibrary.BLOSC_SHUFFLE;
		Sheader sheader = bw.newSchunk(sparams);
		/* Now append a couple of chunks */
		int nchunks = bw.appendBuffer(sheader, PrimitiveSizes.DOUBLE_FIELD_SIZE, iBufferSize, ibb);
		assert (nchunks == 1);
		nchunks = bw.appendBuffer(sheader, PrimitiveSizes.DOUBLE_FIELD_SIZE, iBufferSize, ibb);
		assert (nchunks == 2);

		/* Retrieve and decompress the chunks (0-based count) */
		int dsize = bw.decompressChunk(sheader, 0, obb, iBufferSize);
		if (dsize < 0) {
			System.out.println("Decompression error.  Error code: " + dsize);
			return;
		}
		dsize = bw.decompressChunk(sheader, 1, obb, iBufferSize);
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
		bw.decompress(obb, abb, iBufferSize);

		bw.destroySchunk(sheader);

		double[] data_again = Util.byteBufferToDoubleArray(abb);
		bw.destroy();
		System.out.println(
				"Items Original " + data.length + ", Items compressed " + w + ", Items again " + data_again.length);
		System.out.println("Finished!");
	}
}
