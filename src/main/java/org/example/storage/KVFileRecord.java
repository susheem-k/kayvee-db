package org.example.storage;

public class KVFileRecord {

    public String k;
    public String v;
    public long byteOffset;

    public KVFileRecord(String k, String v, long byteOffset) {
        this.k = k;
        this.v = v;
        this.byteOffset = byteOffset;
    }

}
