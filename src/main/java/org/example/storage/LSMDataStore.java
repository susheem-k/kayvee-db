package org.example.storage;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;

public class LSMDataStore implements DataStore {

    private final LinkedList<MonoFileDataStore> monoFileDataStores;
    private final long perFileSizeLimit;
    private final String dataDir;

    public LSMDataStore(final String dataDir1, long perFileSizeLimit, boolean wipeDir) throws IOException {
        this.perFileSizeLimit = perFileSizeLimit;
        this.dataDir = dataDir1;
        File dataDirectory = new File(dataDir);
        if(!dataDirectory.exists() || !dataDirectory.isDirectory()) {
            throw new RuntimeException(dataDir + " is not a valid directory");
        }
        File[] files = dataDirectory.listFiles();
        if(files.length != 0 && wipeDir) {
            Arrays.stream(files).forEach(file -> file.delete());
        }
        files = new File[0];
        Arrays.sort(files, Comparator.comparing(File::getName));
        monoFileDataStores = new LinkedList<>();
        if(files.length == 0) {
            monoFileDataStores.add(new MonoFileDataStore(dataDir + "/datafile_" + System.currentTimeMillis(), true));
        } else {
            for(File file : files) {
                monoFileDataStores.add(new MonoFileDataStore(file));
            }
        }
    }

    @Override
    public KVFileRecord put(String key, String value) throws IOException {
        if(monoFileDataStores.getLast().getSize() > this.perFileSizeLimit) {
            this.monoFileDataStores.add(new MonoFileDataStore(this.dataDir + "/datafile_" + System.currentTimeMillis(), true));
        }
        return monoFileDataStores.getLast().put(key, value);
    }

    @Override
    public KVFileRecord get(String key) throws IOException {
        return monoFileDataStores.getLast().get(key);
    }

    @Override
    public boolean delete(String key) throws IOException {
        if(monoFileDataStores.getLast().getSize() > this.perFileSizeLimit) {
            this.monoFileDataStores.add(new MonoFileDataStore(this.dataDir + "/datafile_" + System.currentTimeMillis(), true));
        }
        return monoFileDataStores.getLast().delete(key);
    }

    @Override
    public long getSize() throws IOException {
        long sum = 0L;
        for (MonoFileDataStore monoFileDataStore : this.monoFileDataStores) {
            long size = monoFileDataStore.getSize();
            sum += size;
        }
        return sum;
    }
}
