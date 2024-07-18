package org.example.storage;

public interface LockableFileStoreContainer extends MultiFileStoreContainer {

    boolean lockFileStore();
    boolean unlockFileStore();

}
