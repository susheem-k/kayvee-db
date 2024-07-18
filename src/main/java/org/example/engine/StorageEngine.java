package org.example.engine;

import java.io.IOException;

public interface StorageEngine {

    String get(String key);
    void put(String key, String value) throws IOException;
    void delete(String key) throws IOException;

}
