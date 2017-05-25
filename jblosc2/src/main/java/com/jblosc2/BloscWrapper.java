package com.jblosc2;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

import com.jblosc2.jna.BloscLibrary;
import com.jblosc2.jna.ContextCparams;
import com.jblosc2.jna.ContextDparams;
import com.jblosc2.jna.Sheader;
import com.jblosc2.jna.Sparams;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.NativeLongByReference;
import com.sun.jna.ptr.PointerByReference;

/**
 * Class that adds a more Java oriented programming vision to the JNA interface
 * 
 * @author aalted
 *
 */
public class BloscWrapper {

	public static final int OVERHEAD = 16;

	/**
	 * Call to the JNA blosc_init()
	 */
	public void init() {
		BloscLibrary.blosc_init();
	}

	/**
	 * Call to the JNA blosc_destroy()
	 */
	public void destroy() {
		BloscLibrary.blosc_destroy();
	}

	/**
	 * Call to the JNA blosc_set_nthreads
	 * 
	 * @param nthreads
	 */
	public void setNumThreads(int nthreads) {
		BloscLibrary.blosc_set_nthreads(nthreads);
	}

	/**
	 * call to the JNA blosc_get_nthreads
	 * 
	 * @return
	 */
	public int getNumThreads() {
		return BloscLibrary.blosc_get_nthreads();
	}

	/**
	 * Call the JNA blosc_list_compressors
	 * 
	 * @return
	 */
	public String listCompressors() {
		return BloscLibrary.blosc_list_compressors();
	}

	/**
	 * Call to the JNA blosc_set_compressor
	 * 
	 * @param compname
	 */
	public void setCompressor(String compname) {
		BloscLibrary.blosc_set_compressor(compname);
	}

	/**
	 * Call to the JNA blosc_get_compressor
	 * 
	 * @return
	 */
	public String getCompressor() {
		return BloscLibrary.blosc_get_compressor();
	}

	/**
	 * Call to the JNA blosc_compname_to_compcode
	 * 
	 * @param compname
	 * @return
	 */
	public int compnameToCompcode(String compname) {
		return BloscLibrary.blosc_compname_to_compcode(compname);
	}

	/**
	 * Call to the JNA blosc_compcode_to_compname
	 * 
	 * @param compcode
	 * @return
	 */
	public String compcodeToCompname(int compcode) {
		PointerByReference ptr = new PointerByReference();
		BloscLibrary.blosc_compcode_to_compname(compcode, ptr);
		Pointer p = ptr.getValue();
		return p.getString(0);
	}

	/**
	 * Call to the JNA blosc_get_version_string
	 * 
	 * @return
	 */
	public String getVersionString() {
		return BloscLibrary.blosc_get_version_string();
	}

	/**
	 * Call to the JNA blosc_get_complib_info If compname is wrong then
	 * unchecked IllegalArgumentException is thrown
	 * 
	 * @param compname
	 * @return a 2 elements array: 0 -> complib, 1 -> version
	 */
	String[] getComplibInfo(String compname) {
		PointerByReference ptrComplib = new PointerByReference();
		PointerByReference ptrVersion = new PointerByReference();
		int compcode = BloscLibrary.blosc_get_complib_info(compname, ptrComplib, ptrVersion);
		if (compcode == -1) {
			throw new IllegalArgumentException();
		}
		String[] result = new String[2];
		result[0] = ptrComplib.getValue().getString(0);
		result[1] = ptrVersion.getValue().getString(0);
		return result;
	}

	/**
	 * Call to the JNA blosc_free_resources throws an uncheked RuntimeException
	 * if there are problems freeing resources
	 */
	public void freeResources() {
		if (BloscLibrary.blosc_free_resources() == -1) {
			throw new RuntimeException();
		}
	}

	public BufferSizes cbufferSizes(Buffer cbuffer) {
		NativeLongByReference nbytes = new NativeLongByReference();
		NativeLongByReference cbytes = new NativeLongByReference();
		NativeLongByReference blocksize = new NativeLongByReference();
		BloscLibrary.blosc_cbuffer_sizes(cbuffer, nbytes, cbytes, blocksize);
		BufferSizes bs = new BufferSizes(nbytes.getValue().longValue(), cbytes.getValue().longValue(),
				blocksize.getValue().longValue());
		return bs;
	}

	private void checkSizes(long srcLength, long destLength) {
		if (srcLength > (Integer.MAX_VALUE - BloscWrapper.OVERHEAD)) {
			throw new IllegalArgumentException("Source array is too large");
		}
		if (destLength < (srcLength + BloscWrapper.OVERHEAD)) {
			throw new IllegalArgumentException("Dest array is not large enough.");
		}
	}

	private void checkExit(int w) {
		if (w == 0) {
			throw new RuntimeException("Compressed size larger then dest length");
		}
		if (w == -1) {
			throw new RuntimeException("Error compressing data");
		}
	}

	public int compress(int compressionLevel, int shuffleType, int typeSize, ByteBuffer src, long srcLength,
			ByteBuffer dest, long destLength) {
		checkSizes(srcLength, destLength);
		src.position(0);
		dest.position(0);
		src.order(ByteOrder.nativeOrder());
		dest.order(ByteOrder.nativeOrder());
		int w = BloscLibrary.blosc_compress(compressionLevel, shuffleType, new NativeLong(typeSize),
				new NativeLong(srcLength), src, dest, new NativeLong(destLength));
		checkExit(w);
		return w;
	}

	public int decompress(Buffer src, Buffer dest, long destSize) {
		src.position(0);
		dest.position(0);
		return BloscLibrary.blosc_decompress(src, dest, new NativeLong(destSize));
	}

	public int compressCtx(ContextCparams context, Buffer src, long srcLength, Buffer dest, long destLength) {
		src.position(0);
		dest.position(0);
		PointerByReference byref = BloscLibrary.blosc2_create_cctx(context);
		int w = BloscLibrary.blosc2_compress_ctx(byref, new NativeLong(srcLength), src, dest,
				new NativeLong(destLength));
		checkExit(w);
		return w;
	}

	public int decompressCtx(ContextDparams context, Buffer src, Buffer dest, int destSize) {
		src.position(0);
		dest.position(0);
		PointerByReference byref = BloscLibrary.blosc2_create_dctx(context);
		return BloscLibrary.blosc2_decompress_ctx(byref, src, dest, new NativeLong(destSize));
	}

	public int getitemCtx(ContextDparams context, Buffer src, int start, int nitems, Buffer dest) {
		PointerByReference byref = BloscLibrary.blosc2_create_dctx(context);
		return BloscLibrary.blosc2_getitem_ctx(byref, src, start, nitems, dest);
	}

	public void cbufferMetainfo(Buffer cbuffer, NativeLongByReference typesize, IntBuffer flags) {
		BloscLibrary.blosc_cbuffer_metainfo(cbuffer, typesize, flags);
	}

	public void cbufferVersions(Buffer cbuffer, IntBuffer version, IntBuffer versionlz) {
		BloscLibrary.blosc_cbuffer_versions(cbuffer, version, versionlz);
	}

	public Buffer cbufferComplib(Buffer cbuffer) {
		return BloscLibrary.blosc_cbuffer_complib(cbuffer);
	}

	public int getItem(Buffer src, int start, int nitems, Buffer dest) {
		return BloscLibrary.blosc_getitem(src, start, nitems, dest);
	}

	public Sheader newSchunk(Sparams sparams) {
		return BloscLibrary.blosc2_new_schunk(sparams);
	}

	public int appendBuffer(Sheader sheader, int typesize, int nbytes, Buffer src) {
		NativeLong nl = BloscLibrary.blosc2_append_buffer(sheader, new NativeLong(typesize), new NativeLong(nbytes),
				src);
		return nl.intValue();
	}

	public int decompressChunk(Sheader sheader, long nchunk, Buffer dest, int nbytes) {
		return BloscLibrary.blosc2_decompress_chunk(sheader, nchunk, dest, nbytes);
	}

	public int destroySchunk(Sheader sheader) {
		return BloscLibrary.blosc2_destroy_schunk(sheader);
	}

	public int setDeltaRef(Sheader sheader, NativeLong nbytes, Buffer ref) {
		return BloscLibrary.blosc2_set_delta_ref(sheader, nbytes, ref);
	}

	public Buffer packedAppendBuffer(Buffer packed, int typesize, long nbytes, Buffer src) {
		return BloscLibrary.blosc2_packed_append_buffer(packed, new NativeLong(typesize), new NativeLong(nbytes), src);
	}

	public int packedDecompressChunk(Buffer packed, int nchunk, ByteBuffer dest) {
		PointerByReference ptr = new PointerByReference();
		int size = BloscLibrary.blosc2_packed_decompress_chunk(packed, nchunk, ptr);
		Pointer p = ptr.getValue();
		dest.put(p.getByteBuffer(0, size));
		return size;
	}

	public Buffer packSchunk(Sheader sheader) {
		return BloscLibrary.blosc2_pack_schunk(sheader);
	}

	public Sheader unpackSchunk(Buffer packed) {
		return BloscLibrary.blosc2_unpack_schunk(packed);
	}

	public void freeCtx(ContextCparams context) {
		PointerByReference ptrByRef = new PointerByReference();
		Pointer ptr = context.getPointer();
		ptrByRef.setPointer(ptr);
		BloscLibrary.blosc2_free_ctx(ptrByRef);
	}

	public void freeCtx(ContextDparams context) {
		PointerByReference ptrByRef = new PointerByReference();
		Pointer ptr = context.getPointer();
		ptrByRef.setPointer(ptr);
		BloscLibrary.blosc2_free_ctx(ptrByRef);
	}

	public int getBlocksize() {
		return BloscLibrary.blosc_get_blocksize();
	}

	public void setBlocksize(long blocksize) {
		BloscLibrary.blosc_set_blocksize(new NativeLong(blocksize));
	}

	public void setSchunk(Sheader schunk) {
		BloscLibrary.blosc_set_schunk(schunk);
	}

}
