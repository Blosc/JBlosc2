package org.blosc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.junit.Test;

import org.blosc.JBlosc2;
import org.blosc.BufferSizes;
import org.blosc.PrimitiveSizes;
import org.blosc.Shuffle;
import org.blosc.Util;
import org.blosc.jna.BloscLibrary;
import org.blosc.jna.ContextCParams;
import org.blosc.jna.ContextDParams;
import org.blosc.jna.Sheader;
import org.blosc.jna.Sparams;
import com.sun.jna.Memory;
import com.sun.jna.NativeLong;
import com.sun.jna.ptr.NativeLongByReference;

public class BloscTest {

	@Test
	public void testCompressDecompressJNA() {
		int SIZE = 100 * 100 * 100;
		float data[] = new float[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = i * 2;
		}
		float data_out[] = new float[SIZE];
		long isize = SIZE * 4;
		Memory m = new Memory(isize);
		m.write(0, data, 0, SIZE);
		Memory m2 = new Memory(isize);
		BloscLibrary.blosc_init();
		int size = BloscLibrary.blosc_compress(5, 1, new NativeLong(4), new NativeLong(isize),
				m.getByteBuffer(0, isize), m2.getByteBuffer(0, isize), new NativeLong(isize));
		data_out = m2.getFloatArray(0, SIZE);
		Memory m3 = new Memory(isize);
		BloscLibrary.blosc_decompress(m2.getByteBuffer(0, isize), m3.getByteBuffer(0, isize), new NativeLong(isize));
		float[] data_in = m3.getFloatArray(0, SIZE);
		assertArrayEquals(data, data_in, (float) 0);
		BloscLibrary.blosc_destroy();
		assertNotNull(data_out);
		assertTrue(size < isize);
	}

	@Test
	public void testSetCompressor() {
		System.out.println("*** testSetCompressor ***");
		int SIZE = 26214400;
		char data[] = new char[SIZE];
		for (int i = 0; i < SIZE; i++) {
			// data[i] = Math.random();
			data[i] = (char) i;
		}
		ByteBuffer b = Util.array2ByteBuffer(data);
		JBlosc2 jb2 = new JBlosc2();
		System.out.println("Blosc version " + jb2.getVersionString());
		jb2.setNumThreads(4);
		System.out.println("Working with " + jb2.getNumThreads() + " threads");
		assertEquals(jb2.getNumThreads(), 4);
		String compnames = jb2.listCompressors();
		String compnames_array[] = compnames.split(",");
		for (String compname : compnames_array) {
			jb2.setCompressor(compname);
			String compname_out = jb2.getCompressor();
			assertEquals(compname, compname_out);
			String[] ci = jb2.getComplibInfo(compname);
			int compcode = jb2.compnameToCompcode(compname);
			compname_out = jb2.compcodeToCompname(compcode);
			assertEquals(compname, compname_out);
			System.out
					.println("Working with compressor " + compname + " (code " + compcode + ") " + ci[0] + " " + ci[1]);
			long startTime = System.currentTimeMillis();
			ByteBuffer o = ByteBuffer.allocateDirect(SIZE * 2 + JBlosc2.OVERHEAD);
			// int s = jb2.compressCtx(5, Shuffle.BYTE_SHUFFLE,
			// PrimitiveSizes.DOUBLE_FIELD_SIZE, b, SIZE * 8, o,
			// SIZE * 8 + JBlosc2.OVERHEAD, compname, 0, 1);
			jb2.compress(5, Shuffle.BYTE_SHUFFLE, PrimitiveSizes.CHAR_FIELD_SIZE, b,
					SIZE * PrimitiveSizes.CHAR_FIELD_SIZE, o,
					SIZE * PrimitiveSizes.CHAR_FIELD_SIZE + JBlosc2.OVERHEAD);
			long stopTime = System.currentTimeMillis();
			long elapsedTime = stopTime - startTime;
			jb2.cbufferComplib(o);
			// System.out.println("Complib " + complib.array());
			IntBuffer version = IntBuffer.allocate(1);
			IntBuffer versionlz = IntBuffer.allocate(1);
			jb2.cbufferVersions(o, version, versionlz);
			System.out.println("Versions " + version.get(0) + ", " + versionlz.get(0));
			NativeLongByReference typesize = new NativeLongByReference();
			IntBuffer flags = IntBuffer.allocate(1);
			jb2.cbufferMetainfo(o, typesize, flags);
			System.out.println("Metainfo " + typesize.getValue() + ", " + flags.get(0));
			printRatio(jb2, "Char Array", o);
			BufferSizes bs = jb2.cbufferSizes(o);
			double mb = bs.getNbytes() * 1.0 / (1024 * 1024);
			System.out.println("Compress time " + elapsedTime + " ms. "
					+ String.format("%.2f", (mb / elapsedTime) * 1000) + " Mb/s");
			startTime = System.currentTimeMillis();
			ByteBuffer a = ByteBuffer.allocateDirect(SIZE * 2);
			jb2.decompress(o, a, SIZE * 2);
			stopTime = System.currentTimeMillis();
			elapsedTime = stopTime - startTime;
			mb = (bs.getNbytes() * 1.0) / (1024 * 1024);
			char[] data_again = Util.byteBufferToCharArray(a);
			System.out.println("Decompress time " + elapsedTime + " ms. "
					+ String.format("%.2f", (mb / elapsedTime) * 1000) + " Mb/s");
			assertArrayEquals(data, data_again);
		}
		jb2.setBlocksize(256 * 1024);
		int bs = jb2.getBlocksize();
		assertTrue(256 * 1024 == bs);
		jb2.freeResources();
		jb2.destroy();
	}

	private void printRatio(JBlosc2 jb2, String title, ByteBuffer cbuffer) {
		BufferSizes bs = jb2.cbufferSizes(cbuffer);
		System.out.println(title + ": " + bs.getCbytes() + " from " + bs.getNbytes() + ". Ratio: "
				+ (String.format("%.2f", (0.0 + bs.getNbytes()) / bs.getCbytes())));
	}

	@Test
	public void testCompressDecompressDouble() {
		int SIZE = 100 * 100 * 100;
		double data[] = new double[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = i * 2;
		}
		ByteBuffer ibb = Util.array2ByteBuffer(data);
		JBlosc2 jb2 = new JBlosc2();
		ByteBuffer obb = ByteBuffer.allocateDirect(ibb.limit() + JBlosc2.OVERHEAD);
		jb2.compress(5, Shuffle.BYTE_SHUFFLE, PrimitiveSizes.DOUBLE_FIELD_SIZE, ibb, ibb.limit(), obb, obb.limit());
		printRatio(jb2, "Double", obb);
		ByteBuffer abb = ByteBuffer.allocateDirect(ibb.limit());
		jb2.decompress(obb, abb, abb.limit());
		double[] data_again = Util.byteBufferToDoubleArray(abb);
		jb2.destroy();
		assertArrayEquals(data, data_again, (float) 0);
	}

	@Test
	public void testCompressDecompressFloat() {
		int SIZE = 100 * 100 * 100;
		float data[] = new float[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = i * 2;
		}
		ByteBuffer ibb = Util.array2ByteBuffer(data);
		JBlosc2 jb2 = new JBlosc2();
		ByteBuffer obb = ByteBuffer.allocateDirect(ibb.limit() + JBlosc2.OVERHEAD);
		jb2.compress(5, Shuffle.BYTE_SHUFFLE, PrimitiveSizes.FLOAT_FIELD_SIZE, ibb, ibb.limit(), obb, obb.limit());
		printRatio(jb2, "Float", obb);
		ByteBuffer abb = ByteBuffer.allocateDirect(ibb.limit());
		jb2.decompress(obb, abb, abb.limit());
		float[] data_again = Util.byteBufferToFloatArray(abb);
		jb2.destroy();
		assertArrayEquals(data, data_again, (float) 0);
	}

	@Test
	public void testCompressDecompressLong() {
		int SIZE = 100 * 100 * 100;
		long data[] = new long[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = i * 2;
		}
		ByteBuffer ibb = Util.array2ByteBuffer(data);
		JBlosc2 jb2 = new JBlosc2();
		ByteBuffer obb = ByteBuffer.allocateDirect(ibb.limit() + JBlosc2.OVERHEAD);
		jb2.compress(5, Shuffle.BYTE_SHUFFLE, PrimitiveSizes.LONG_FIELD_SIZE, ibb, ibb.limit(), obb, obb.limit());
		printRatio(jb2, "Long", obb);
		ByteBuffer abb = ByteBuffer.allocateDirect(ibb.limit());
		jb2.decompress(obb, abb, abb.limit());
		long[] data_again = Util.byteBufferToLongArray(abb);
		jb2.destroy();
		assertArrayEquals(data, data_again);
	}

	@Test
	public void testCompressDecompressInt() {
		int SIZE = 100 * 100 * 100;
		int data[] = new int[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = i * 2;
		}
		ByteBuffer ibb = Util.array2ByteBuffer(data);
		JBlosc2 jb2 = new JBlosc2();
		ByteBuffer obb = ByteBuffer.allocateDirect(ibb.limit() + JBlosc2.OVERHEAD);
		jb2.compress(5, Shuffle.BYTE_SHUFFLE, PrimitiveSizes.INT_FIELD_SIZE, ibb, ibb.limit(), obb, obb.limit());
		printRatio(jb2, "Int", obb);
		ByteBuffer abb = ByteBuffer.allocateDirect(ibb.limit());
		jb2.decompress(obb, abb, abb.limit());
		int[] data_again = Util.byteBufferToIntArray(abb);
		jb2.destroy();
		assertArrayEquals(data, data_again);
	}

	@Test
	public void testCompressDecompressDoubleCtx() {
		int SIZE = 100 * 100 * 100;
		double data[] = new double[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = i;
		}
		ByteBuffer ibb = Util.array2ByteBuffer(data);
		JBlosc2 jb2 = new JBlosc2();
		ByteBuffer obb = ByteBuffer.allocateDirect(ibb.limit() + JBlosc2.OVERHEAD);
		ContextCParams cctx = new ContextCParams();
		cctx.clevel = 5;
		cctx.filtercode = BloscLibrary.BLOSC_DOSHUFFLE;
		cctx.compcode = BloscLibrary.BLOSC_LZ4;
		cctx.typesize = PrimitiveSizes.DOUBLE_FIELD_SIZE;
		cctx.nthreads = 2;
		// cctx.blocksize = 256 * 1024;
		// cctx.nthreads = 1;
		jb2.compressCtx(cctx, ibb, ibb.limit(), obb, obb.limit());
		printRatio(jb2, "Double", obb);
		ByteBuffer abb = ByteBuffer.allocateDirect(ibb.limit());
		ContextDParams dctx = new ContextDParams();
		ByteBuffer subsetbb = ByteBuffer.allocateDirect(5 * PrimitiveSizes.DOUBLE_FIELD_SIZE);
		int ret = jb2.getitemCtx(dctx, obb, 5, 5, subsetbb);
		assertTrue(ret >= 0);
		double data_subset_ref[] = { 5, 6, 7, 8, 9 };
		for (int i = 0; i < 5; i++) {
			assertTrue(subsetbb.getDouble(i * PrimitiveSizes.DOUBLE_FIELD_SIZE) == data_subset_ref[i]);
		}
		dctx.nthreads = 2;
		jb2.decompressCtx(dctx, obb, abb, abb.limit());
		double[] data_again = Util.byteBufferToDoubleArray(abb);
		jb2.destroy();
		assertArrayEquals(data, data_again, (float) 0);
	}

	@Test
	public void testCompressDecompressDirectBuffer() {
		int SIZE = 100 * 100 * 100;
		ByteBuffer ibb = ByteBuffer.allocateDirect(SIZE * PrimitiveSizes.DOUBLE_FIELD_SIZE);
		for (int i = 0; i < SIZE; i++) {
			ibb.putDouble(i);
		}
		JBlosc2 jb2 = new JBlosc2();
		ByteBuffer obb = ByteBuffer.allocateDirect(ibb.limit() + JBlosc2.OVERHEAD);
		jb2.compress(5, Shuffle.BYTE_SHUFFLE, PrimitiveSizes.DOUBLE_FIELD_SIZE, ibb, ibb.limit(), obb, obb.limit());
		printRatio(jb2, "Double", obb);
		ByteBuffer abb = ByteBuffer.allocateDirect(ibb.limit());
		jb2.decompress(obb, abb, abb.limit());
		jb2.destroy();
		assertEquals(ibb, abb);
	}

	public void testChunks() {
		int SIZE = 100 * 100 * 100;
		double data[] = new double[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = i;
		}
		ByteBuffer ibb = Util.array2ByteBuffer(data);
		JBlosc2 jb2 = new JBlosc2();
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
		assertEquals(nchunks, 1);
		nchunks = jb2.appendBuffer(sheader, PrimitiveSizes.DOUBLE_FIELD_SIZE, iBufferSize, ibb);
		assertEquals(nchunks, 2);

		/* Retrieve and decompress the chunks (0-based count) */
		int dsize = jb2.decompressChunk(sheader, 0, obb, iBufferSize);
		assertTrue(dsize > 0);
		dsize = jb2.decompressChunk(sheader, 1, obb, iBufferSize);
		assertTrue(dsize > 0);

		for (int i = 0; i < SIZE; i++) {
			if (ibb.getDouble(i) != obb.getDouble(i)) {
				// printf("i, values: %d, %f, %f\n", i, data[i], data_dest[i]);
				System.out.println("Values " + ibb.getDouble(i) + ", " + obb.getDouble(i));
				System.out.println("Decompressed data differs from original at " + i + "!");
				fail("Decompressed data differs from original at " + i + "!");
			}
		}

		ByteBuffer abb = ByteBuffer.allocateDirect(iBufferSize);
		jb2.decompress(obb, abb, iBufferSize);

		jb2.destroySchunk(sheader);
		jb2.destroy();
	}

	public void testDeltaPackedChunks() {
		int SIZE = 100 * 100 * 100;
		double data[] = new double[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = i;
		}
		ByteBuffer ibb = Util.array2ByteBuffer(data);
		JBlosc2 jb2 = new JBlosc2();
		int iBufferSize = SIZE * PrimitiveSizes.DOUBLE_FIELD_SIZE;
		int oBufferSize = SIZE * PrimitiveSizes.DOUBLE_FIELD_SIZE + JBlosc2.OVERHEAD;
		ByteBuffer obb = ByteBuffer.allocateDirect(oBufferSize);

		Sparams sparams = new Sparams();
		sparams.filters[0] = BloscLibrary.BLOSC_DELTA;
		sparams.filters[1] = BloscLibrary.BLOSC_SHUFFLE;
		Sheader sheader = jb2.newSchunk(sparams);

		int nchunks = jb2.appendBuffer(sheader, PrimitiveSizes.DOUBLE_FIELD_SIZE, iBufferSize, ibb);
		assertTrue(nchunks == 1);

		assertTrue(sheader.nbytes > 0);
		assertTrue(sheader.cbytes > 0);

		ByteBuffer packed = (ByteBuffer) jb2.packSchunk(sheader);
		Sheader sheader2 = jb2.unpackSchunk(packed);
		sheader2.clear();

		/* Now append another chunk (essentially the same as the reference) */
		packed = (ByteBuffer) jb2.packedAppendBuffer(packed, PrimitiveSizes.DOUBLE_FIELD_SIZE, iBufferSize, ibb);

		long dsize = jb2.packedDecompressChunk(packed, 1, obb);
		assertTrue(dsize > 0);

		for (int i = 0; i < SIZE; i++) {
			if (ibb.getDouble(i) != obb.getDouble(i)) {
				System.out.println("Values " + ibb.getDouble(i) + ", " + obb.getDouble(i));
				System.out.println("Decompressed data differs from original at " + i + "!");
				fail("Decompressed data differs from original at " + i + "!");
			}
		}

		jb2.destroySchunk(sheader);
		jb2.destroy();
	}
}