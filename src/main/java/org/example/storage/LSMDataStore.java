package org.example.storage;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

public class LSMDataStore implements DataStore {

    private final long perFileSizeLimit;
    private final MultiFileStoreContainer monoFileDataStores;

    public LSMDataStore(long perFileSizeLimit, MultiFileStoreContainer monoFileDataStores) throws IOException {
        this.perFileSizeLimit = perFileSizeLimit;
        this.monoFileDataStores = monoFileDataStores;
    }

    @Override
    public KVFileRecord put(String key, String value) throws IOException {
        if(this.monoFileDataStores.getDataFiles().getLast().getSize() > this.perFileSizeLimit) {
            this.monoFileDataStores.createNew("datafile_" + System.currentTimeMillis());
        }
        return this.monoFileDataStores.getDataFiles().getLast().put(key, value);
    }

    @Override
    public KVFileRecord get(String key) throws IOException {
        return this.monoFileDataStores.getDataFiles().getLast().get(key);
    }

    @Override
    public boolean delete(String key) throws IOException {
        if(this.monoFileDataStores.getDataFiles().getLast().getSize() > this.perFileSizeLimit) {
            this.monoFileDataStores.createNew("datafile_" + System.currentTimeMillis());
        }
        return this.monoFileDataStores.getDataFiles().getLast().delete(key);
    }

    @Override
    public long getSize() throws IOException {
        long sum = 0L;
        for (MonoFileDataStore monoFileDataStore : this.monoFileDataStores.getDataFiles()) {
            long size = monoFileDataStore.getSize();
            sum += size;
        }
        return sum;
    }
}
