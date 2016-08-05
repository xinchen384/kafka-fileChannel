/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.network;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.GatheringByteChannel;
//

import java.util.Arrays;
import java.security.MessageDigest;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.FileInputStream;
import java.nio.channels.WritableByteChannel;
import java.nio.ByteBuffer;

public class MMyFileChannel {
 
  private FileChannel fileChannel = null;

  public void openChannel(File file, boolean mutable)throws IOException{
    openChannel(file, mutable, false, 0, false);
  }
  public void openChannel(File file, boolean mutable, boolean fileAlreadyExists, int initFileSize, boolean preallocate)throws IOException{
    if (mutable){
      if (fileAlreadyExists)
        fileChannel = new RandomAccessFile(file, "rw").getChannel();
      else{
        if (preallocate) {
          RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
          randomAccessFile.setLength(initFileSize);
          fileChannel = randomAccessFile.getChannel();
          System.out.println(" XIN open a random file with init size: " + initFileSize);
        }
        else{
          fileChannel = new RandomAccessFile(file, "rw").getChannel();
          System.out.println(" XIN open a random file with name: " + file.getAbsolutePath());
        }
      }
    }
    else
      fileChannel = new FileInputStream(file).getChannel();
  }

  public long transferTo(long pos, long count, WritableByteChannel dest) throws IOException{
    //System.out.println(" calling transferTo in my file channel!!! ");
    long tbytes = fileChannel.transferTo(pos, count, dest);
    System.out.println(" XIN sendfile channel count " + count + " offset: " + pos + " sent size: " + tbytes);
    return tbytes;
  }
  public FileChannel truncate(long tSize)throws IOException{
    System.out.println(" calling truncate in my file channel!!! ");
    return fileChannel.truncate(tSize);
  }
  
  public long read(ByteBuffer buffer, int pos)throws IOException{
    int rbytes = fileChannel.read(buffer, pos);
    System.out.println(" XIN read file channel size " + size() + " offset: " + pos + " read size: " + rbytes);
    return (long)rbytes;
  }
  public int write(ByteBuffer buffer)throws IOException{
    int written = fileChannel.write(buffer);
    System.out.println(" XIN write  my file channel with size " + size() + " written size: " + written);
    return written;
  }

  public long size()throws IOException{
    return fileChannel.size();
  }
  public FileChannel position(int start)throws IOException{
    return fileChannel.position(start);
  }
  public void force(boolean sign)throws IOException{
    fileChannel.force(sign); 
  }
  public void close()throws IOException{
    fileChannel.close();
  }
  
}
