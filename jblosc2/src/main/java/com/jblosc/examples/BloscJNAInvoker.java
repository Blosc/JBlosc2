package com.jblosc.examples;

import java.nio.ByteBuffer;

import com.jblosc.jna.BloscLibrary;
import com.sun.jna.Memory;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class BloscJNAInvoker {

	public static void main(String[] args) {
		int SIZE = 100 * 100 * 100 * 10;
		byte data[] = new byte[SIZE];
		for (int i = 0; i < SIZE; i++) {
			data[i] = 'a';
		}
		byte dataout[] = new byte[SIZE];
		int isize = SIZE * 1;
		long startTime = System.currentTimeMillis();
		// BloscLibrary iBlosc = (BloscLibrary) Native.loadLibrary("blosc",
		// BloscLibrary.class);
		ByteBuffer bb = ByteBuffer.allocateDirect(isize);
		bb.put(data);
		ByteBuffer bb2 = ByteBuffer.allocateDirect(isize);
		bb.put(data);
		BloscLibrary.blosc_init();
		BloscLibrary.blosc_set_nthreads(2);
		System.out.println("Threads " + BloscLibrary.blosc_get_nthreads());
		int size = BloscLibrary.blosc_compress(5, 1, new NativeLong(4), new NativeLong(isize), bb, bb2,
				new NativeLong(isize));
		long stopTime = System.currentTimeMillis();
		System.out.println("Compress time " + (stopTime - startTime) + " ms");
		System.out.println("Size " + size);
		BloscLibrary.blosc_set_compressor("blosclz");
		System.out.println(BloscLibrary.blosc_get_compressor());
		PointerByReference ptr = new PointerByReference();
		Pointer p = ptr.getValue();
		Memory m3 = new Memory(100);
		m3.setPointer(0, p);
		BloscLibrary.blosc_compcode_to_compname(2, ptr);
		System.out.println("Compname " + ptr.getValue().getString(0));
		int compcode = BloscLibrary.blosc_compname_to_compcode("lz4hc");
		System.out.println("Compcode " + compcode);
		System.out.println("List " + BloscLibrary.blosc_list_compressors());
		System.out.println("List " + BloscLibrary.blosc_get_version_string());
		BloscLibrary.blosc_destroy();
		System.out.println("Finished!");
	}

}
