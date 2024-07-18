package org.example;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.example.engine.IndexedStorageEngine;
import org.example.engine.StorageEngine;
import org.example.storage.DataStore;
import org.example.storage.LSMDataStore;
import org.example.storage.MonoFileDataStore;

import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {
        DataStore dataStore = new LSMDataStore("datadir", 10000, true);
        StorageEngine storageEngine = new IndexedStorageEngine(dataStore, 1000);
        long t0 = System.currentTimeMillis();
        long numRecords = 5000;
        for(int i = 0; i < numRecords; i++) {
            if(new Random().nextBoolean()) {
                dataStore.put("key_" + i, String.valueOf(i));
            } else {
                dataStore.delete("key_" + (i - 1));
            }

        }
        long t1 = System.currentTimeMillis();
        System.out.println("Average Write Time : " + (t1 - t0) / (1.0 * numRecords) + " ms");

        t0 = System.currentTimeMillis();
        for(int i = 0; i < numRecords; i++) {
            dataStore.get("key_" + i);
        }
        t1 = System.currentTimeMillis();
        System.out.println("Average Read Time : " + (t1 - t0) / (1.0 * numRecords) + " ms");
    }

}