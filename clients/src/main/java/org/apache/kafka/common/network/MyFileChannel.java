//package com.sun.jna.examples;
package org.apache.kafka.common.network;

/* HelloWorld.java */
//import com.sun.nio.ch.*;


import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.io.File;
import java.io.IOException;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.OutputStream;

import sun.misc.SharedSecrets;
import sun.misc.Cleaner;
import java.lang.reflect.*;
import sun.nio.ch.DirectBuffer;


//public class HelloWorld {
  
public class MyFileChannel{
    private static class BufferCache {
        private static final int TEMP_BUF_POOL_SIZE = 8;
        // the array of buffers
        private ByteBuffer[] buffers;

        // the number of buffers in the cache
        private int count;

        // the index of the first valid buffer (undefined if count == 0)
        private int start;

        private int next(int i) {
            return (i + 1) % TEMP_BUF_POOL_SIZE;
        }

        BufferCache() {
            buffers = new ByteBuffer[TEMP_BUF_POOL_SIZE];
        }

        /**
         * Removes and returns a buffer from the cache of at least the given
         * size (or null if no suitable buffer is found).
         */
        ByteBuffer get(int size) {
            if (count == 0)
                return null;  // cache is empty

            ByteBuffer[] buffers = this.buffers;

            // search for suitable buffer (often the first buffer will do)
            ByteBuffer buf = buffers[start];
            if (buf.capacity() < size) {
                buf = null;
                int i = start;
                while ((i = next(i)) != start) {
                    ByteBuffer bb = buffers[i];
                    if (bb == null)
                        break;
                    if (bb.capacity() >= size) {
                        buf = bb;
                        break;
                    }
                }
                if (buf == null)
                    return null;
                // move first element to here to avoid re-packing
                buffers[i] = buffers[start];
            }

            // remove first element
            buffers[start] = null;
            start = next(start);
            count--;

            // prepare the buffer and return it
            buf.rewind();
            buf.limit(size);
            return buf;
        }

        boolean offerFirst(ByteBuffer buf) {
            if (count >= TEMP_BUF_POOL_SIZE) {
                return false;
            } else {
                start = (start + TEMP_BUF_POOL_SIZE - 1) % TEMP_BUF_POOL_SIZE;
                buffers[start] = buf;
                count++;
                return true;
            }
        }

        boolean offerLast(ByteBuffer buf) {
            if (count >= TEMP_BUF_POOL_SIZE) {
                return false;
            } else {
                int next = (start + count) % TEMP_BUF_POOL_SIZE;
                buffers[next] = buf;
                count++;
                return true;
            }
        }

        boolean isEmpty() {
            return count == 0;
        }

        ByteBuffer removeFirst() {
            assert count > 0;
            ByteBuffer buf = buffers[start];
            buffers[start] = null;
            start = next(start);
            count--;
            return buf;
        }
    }
    private static ThreadLocal<BufferCache> bufferCache =
        new ThreadLocal<BufferCache>()
    {
        @Override
        protected BufferCache initialValue() {
            return new BufferCache();
        }
    };
    /**
     * Returns a temporary buffer of at least the given size
     */
    static ByteBuffer getTemporaryDirectBuffer(int size) {
        BufferCache cache = bufferCache.get();
        ByteBuffer buf = cache.get(size);
        if (buf != null) {
            return buf;
        } else {
            // No suitable buffer in the cache so we need to allocate a new
            // one. To avoid the cache growing then we remove the first
            // buffer from the cache and free it.
            if (!cache.isEmpty()) {
                buf = cache.removeFirst();
                free(buf);
            }
            return ByteBuffer.allocateDirect(size);
        }
    }
    static void offerFirstTemporaryDirectBuffer(ByteBuffer buf) {
        assert buf != null;
        BufferCache cache = bufferCache.get();
        if (!cache.offerFirst(buf)) {
            // cache is full
            free(buf);
        }
    }
    private static void free(ByteBuffer buf) {
        ((DirectBuffer)buf).cleaner().clean();
    }

    /*
    public interface DirectBuffer {
    public long address();
    public Object attachment();
    public Cleaner cleaner();
    }
    */

    public interface CTest extends Library {
        public void helloFromC();

        public void myopenFile(String fname);
        public int mywrite(long addr, long count);
        public int mypwrite(long addr, long count, long offset);
        public int myread(long addr, long size);
        public int mypread(long addr, long size, long pos);

        public long myposition(long offset);
        public long mytransferTo(long pos, long count, int fd);
        public int mytruncate(long size_t);
 
        public long mysize();
        public void myforce(int sign);
        public void myclose();
        public void mycleanup(Pointer pVals);
    }  
  private CTest ctest = null; 
  public void openChannel(File file, boolean mutable){
    openChannel(file, mutable, false, 0, false);
  }
  public void openChannel(File file, boolean mutable, boolean fileAlreadyExists, int initFileSize, boolean preallocate){
    ctest = (CTest) Native.loadLibrary("ctest", CTest.class);
    ctest.myopenFile( file.getAbsolutePath() );
  }
  /*
  public int read(ByteBuffer buffer, long pos){
    PointerByReference ptrRef = new PointerByReference();
    long bl = buffer.limit();
    int read_bytes;
    read_bytes = ctest.myread(ptrRef, bl, pos);
    Pointer p = ptrRef.getValue();
    //System.out.println("read inside : " + p.getByteArray(0, bl));
    buffer.put(p.getByteArray(0, read_bytes));
    ctest.mycleanup(p);
    return read_bytes;
  }
  */
  
  public int write(ByteBuffer src) {
        if (src instanceof DirectBuffer)
            return writeFromNativeBuffer(src, -1);
        // Substitute a native buffer
        int pos = src.position();
        int lim = src.limit();
        assert (pos <= lim);
        int rem = (pos <= lim ? lim - pos : 0);
        ByteBuffer bb = getTemporaryDirectBuffer(rem);
        try {
            bb.put(src);
            bb.flip();
            // Do not update src until we see how many bytes were written
            src.position(pos);

            int n = writeFromNativeBuffer(bb, -1);
            if (n > 0) {
                // now update src
                src.position(pos + n);
            }
            return n;
        } finally {
            offerFirstTemporaryDirectBuffer(bb);
        }
    //String val = new String(buffer.array());
    //return ctest.mywrite(val);
  }
  private int writeFromNativeBuffer(ByteBuffer bb, long position)
    {
        int pos = bb.position();
        int lim = bb.limit();
        assert (pos <= lim);
        int rem = (pos <= lim ? lim - pos : 0);

        int written = 0;
        if (rem == 0)
            return 0;
        if (position != -1) {
            written = ctest.mypwrite( ((DirectBuffer)bb).address() + pos, rem, position);
        } else {
            written = ctest.mywrite( ((DirectBuffer)bb).address() + pos, rem);
            System.out.println("XIN write buffer!!! with offset: " + position + " full size: " + ctest.mysize() + " written " + written);             
            //System.out.println(" call c write from java ");
        }
        if (written > 0)
            bb.position(pos + written);
        return written;
    }
  public int read(ByteBuffer dst, int pos){
    if (dst instanceof DirectBuffer)
            return readIntoNativeBuffer( dst, pos);
        // Substitute a native buffer
        ByteBuffer bb = getTemporaryDirectBuffer(dst.remaining());
        try {
            int n = readIntoNativeBuffer( bb, pos );
            bb.flip();
            if (n > 0)
                dst.put(bb);
            return n;
        } finally {
            offerFirstTemporaryDirectBuffer(bb);
        }
  }
  private int readIntoNativeBuffer( ByteBuffer bb, int position )
    {
        int pos = bb.position();
        int lim = bb.limit();
        assert (pos <= lim);
        int rem = (pos <= lim ? lim - pos : 0);

        if (rem == 0)
            return 0;
        int n = 0;
        if (position != -1) {
            n = ctest.mypread( ((DirectBuffer)bb).address() + pos, rem, position);
            System.out.println("XIN read native buffer!!! with offset: " + position + " full size: " + ctest.mysize() + " read " + n);             
        } else {
            n = ctest.myread( ((DirectBuffer)bb).address() + pos, rem);
        }
        if (n > 0)
            bb.position(pos + n);
        return n;
    }

  ////////////////////
  public long size(){
    return Long.valueOf(ctest.mysize());
  }
  public void force(boolean sign){
    int myInt = (sign)? 1:0;
    ctest.myforce(myInt);
  }
  public void close(){
    ctest.myclose();
  }
  ///////////////////
  
  public long transferTo(long pos, long count, WritableByteChannel target) throws IOException{
        // search "get Unix File Descriptor in Java" in google
        OutputStream outputStream = Channels.newOutputStream(target);
        FileDescriptor targetFD = ((FileOutputStream)outputStream).getFD();
        int fd = SharedSecrets.getJavaIOFileDescriptorAccess().get(targetFD);
        return ctest.mytransferTo(pos, count, fd);
  }
  public long position(long offset){
        return ctest.myposition(offset);
  }
  //public FileChannel position(long offset){
  //      ctest.myposition(offset); 
  //      return null;
  //} 
  public int truncate(long tSize){
        return ctest.mytruncate(tSize); 
  }

  /*
  public FileChannel position(int start)throws IOException{
    return fileChannel.position(start);
  }
  public long transferTo(long pos, long count, WritableByteChannel dest) throws IOException{
    System.out.println(" calling transferTo in my file channel!!! ");
    return fileChannel.transferTo(pos, count, dest);
  }
  public FileChannel truncate(long tSize)throws IOException{
    System.out.println(" calling truncate in my file channel!!! ");
    return fileChannel.truncate(tSize);
  }
  */
  }
 /* 
  static public void main(String argv[]) throws IOException{
        //CTest ctest = (CTest) Native.loadLibrary("ctest", CTest.class);
        //test.helloFromC();
        String k = "abcd123456";
        ByteBuffer b = ByteBuffer.wrap(k.getBytes());
        String v = new String(b.array());

        //MyFileChannel myfile = new MyFileChannel();
        //myfile.openChannel(new File("./test1.txt"), true);
        //myfile.write(b);
        //ByteBuffer s = ByteBuffer.allocate(10);
        //myfile.read(s, 0);
        //System.out.println("read from fileChannel: " + new String(s.array()));

        JTest jtest = new JTest();
        jtest.openChannel(new File("./test2.txt"), true);
        jtest.position(0);

        jtest.write(b);
        ByteBuffer r = ByteBuffer.allocate(10);
        jtest.read(r, 0);
        System.out.println("read from C Channel: " + new String(r.array()));
  }
*/
//}
