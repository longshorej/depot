package depot;

import java.io.IOException;
import java.nio.file.StandardCopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Native {
  // @TODO finalize for Native to remove temporary file when class loader is GCd
  // @TODO extract from resources, based on runtime analysis of platform
  static {
    try {
      Path libdepotDestination = Files.createTempFile("depot", ".so");
      Path libdepotSource =
          Paths.get("/home/longshorej/work/appalachian/depot/jvm/cargo/target/release/libdepot.so");
      Files.copy(libdepotSource, libdepotDestination, StandardCopyOption.REPLACE_EXISTING);
      System.load(libdepotDestination.toAbsolutePath().toString());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static native long queueNew(String path);

  public static native void queueAppend(long ptr, byte[] data);

  public static native void queueDestroy(long ptr);

  public static native boolean queueIsEmpty(long ptr);

  public static native boolean queueIsFull(long ptr);

  public static native long queueStream(long ptr, long id);

  public static native long queueStreamNextItem(long ptr);

  public static native void queueStreamDestroy(long ptr);

  public static native long queueStreamItemId(long ptr);

  public static native int queueStreamItemLength(long ptr);

  public static native void queueStreamItemCopy(long ptr, byte[] data);

  public static native boolean queueStreamItemTruncated(long ptr);

  public static native void queueStreamItemDestroy(long ptr);

  public static native void queueSync(long ptr);

  public static native int queueLastId(long ptr);
}
