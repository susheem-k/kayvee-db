package org.example.storage;

import java.io.IOException;

public interface DataStore {

    KVFileRecord put(String key, String value) throws IOException;
    KVFileRecord get(String key) throws IOException;
    boolean delete(String key) throws IOException;
    long getSize() throws IOException;

}
