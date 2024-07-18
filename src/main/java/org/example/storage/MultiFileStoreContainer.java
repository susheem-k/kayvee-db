package org.example.storage;

import java.io.IOException;
import java.util.LinkedList;

public interface MultiFileStoreContainer {

    LinkedList<MonoFileDataStore> getDataFiles();
    MonoFileDataStore createNew(String name) throws IOException;

}
