package org.example.engine;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.example.storage.DataStore;
import org.example.storage.KVFileRecord;

import java.io.IOException;

public class IndexedStorageEngine implements StorageEngine {

    private final DataStore dataStore;
    private final LoadingCache<String, KVFileRecord> cache;

    public IndexedStorageEngine(DataStore dataStore, int size) {
        this.dataStore = dataStore;
        this.cache = Caffeine.newBuilder().maximumSize(size).build(key -> dataStore.get(key));
    }

    @Override
    public String get(String key) {
        KVFileRecord kvFileRecord = cache.get(key);
        if(kvFileRecord == null) {
            return null;
        }
        return kvFileRecord.v;
    }

    @Override
    public void put(String key, String value) throws IOException {
        KVFileRecord kvFileRecord = dataStore.put(key, value);
        cache.put(key, kvFileRecord);
    }

    @Override
    public void delete(String key) throws IOException {
        dataStore.delete(key);
        cache.invalidate(key);
    }
}
