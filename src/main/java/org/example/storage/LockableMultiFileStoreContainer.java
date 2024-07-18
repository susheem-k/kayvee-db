package org.example.storage;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

public class LockableMultiFileStoreContainer implements LockableFileStoreContainer {

    private final LinkedList<MonoFileDataStore> monoFileDataStores;
    private final String dataDir;
    private final ReentrantLock lock = new ReentrantLock();

    public LockableMultiFileStoreContainer(String dataDir1, boolean wipeDir) throws IOException {
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
    public LinkedList<MonoFileDataStore> getDataFiles() {
        if(lock.isLocked() && !lock.isHeldByCurrentThread()) {
          throw new RuntimeException("File store is currently locked");
        }
        return this.monoFileDataStores;
    }

    @Override
    public MonoFileDataStore createNew(String name) throws IOException {
        if(lock.isLocked() && !lock.isHeldByCurrentThread()) {
            throw new RuntimeException("File store is currently locked");
        }
        MonoFileDataStore fileDataStore = new MonoFileDataStore(dataDir + "/" + name, true);
        this.getDataFiles().add(fileDataStore);
        return fileDataStore;
    }

    @Override
    public boolean lockFileStore() {
        lock.lock();
        return true;
    }

    @Override
    public boolean unlockFileStore() {
        lock.unlock();
        return true;
    }
}
