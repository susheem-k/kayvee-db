package org.example.storage;

import org.apache.commons.io.input.ReversedLinesFileReader;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class MonoFileDataStore implements DataStore {

    private final File file;
    private final RandomAccessFile roFile;
    private final RandomAccessFile rwFile;

    public MonoFileDataStore(final String filePath, boolean createNewFile) throws IOException {
        this.file = new File(filePath);
        if(!file.exists()) {
            file.createNewFile();
        }
        if(createNewFile) {
            file.delete();
            file.createNewFile();
        }
        this.roFile = new RandomAccessFile(this.file, "r");
        this.rwFile = new RandomAccessFile(this.file, "rw");
    }

    public MonoFileDataStore(final File inpFile) throws IOException {
        this.file = inpFile;
        if(!file.exists()) {
            file.createNewFile();
        }
        this.roFile = new RandomAccessFile(this.file, "r");
        this.rwFile = new RandomAccessFile(this.file, "rw");
    }

    @Override
    public KVFileRecord put(String key, String value) throws IOException {
        String formatted = "0," + key + "," + value + "\n";
        FileChannel channel = this.rwFile.getChannel();
        long offset = channel.size();
        channel.position();
        channel.write(ByteBuffer.wrap(formatted.getBytes(StandardCharsets.UTF_8)));
        return new KVFileRecord(key, value, offset);
    }

    @Override
    public KVFileRecord get(String key) throws IOException {
        KVFileRecord kvFileRecord = findRecordOffset(key);
        if(kvFileRecord == null) {
            return null;
        }
        return kvFileRecord;
    }

    @Override
    public boolean delete(String key) throws IOException {
        String formatted = "1," + key + "," + "tombstone" + "\n";
        FileChannel channel = this.rwFile.getChannel();
        channel.position(channel.size());
        channel.write(ByteBuffer.wrap(formatted.getBytes(StandardCharsets.UTF_8)));
        return true;
    }

    @Override
    public long getSize() throws IOException {
        return this.roFile.getChannel().size();
    }

    // todo : read from reverse for speed
    private KVFileRecord findRecordOffset(String key) throws IOException {
        long offset = this.roFile.getChannel().size();
        String line;
        ReversedLinesFileReader reversedLinesFileReader = new ReversedLinesFileReader(this.file);
        while((line = reversedLinesFileReader.readLine()) != null) {
            String[] kvRow = line.split(",");
            if(kvRow[1].equals(key)) {
                if(kvRow[0].equals("1")) {
                    return null;
                }
                return new KVFileRecord(key, kvRow[2], offset - 1 - line.getBytes(StandardCharsets.UTF_8).length);
            }
            offset = offset - line.getBytes(StandardCharsets.UTF_8).length - 1;
        }
        return null;
    }


}
