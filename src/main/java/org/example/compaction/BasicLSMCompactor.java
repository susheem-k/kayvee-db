package org.example.compaction;

import org.example.storage.MultiFileStoreContainer;
import org.example.storage.MonoFileDataStore;

import java.util.LinkedList;

public class BasicLSMCompactor implements LSMDataCompactor {

    private final MultiFileStoreContainer dataFileStore;

    public BasicLSMCompactor(MultiFileStoreContainer dataFileAwareStore) {
        this.dataFileStore = dataFileAwareStore;
    }

    @Override
    public void doCompaction() {
        LinkedList<MonoFileDataStore> monoFileDataStores = this.dataFileStore.getDataFiles();

    }
}
