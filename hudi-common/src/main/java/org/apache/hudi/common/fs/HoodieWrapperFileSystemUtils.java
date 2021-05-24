package org.apache.hudi.common.fs;

import org.apache.hadoop.fs.FileSystem;

public class HoodieWrapperFileSystemUtils {
    private static HoodieWrapperFileSystem hoodieWrapperFileSystem;

    public static HoodieWrapperFileSystem getHoodieWrapperFileSystem(FileSystem fileSystem, ConsistencyGuard consistencyGuard) {
        if (hoodieWrapperFileSystem == null) {
            synchronized (HoodieWrapperFileSystemUtils.class) {
                if (hoodieWrapperFileSystem == null) {
                    hoodieWrapperFileSystem = new HoodieWrapperFileSystem(fileSystem, consistencyGuard);
                }
            }
        }
        return hoodieWrapperFileSystem;
    }
}
