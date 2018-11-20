package depot.section;

import java.nio.file.Path;
import java.util.Iterator;

public class SectionReader {
  private final Path path;
  private final int maxFileSize;
  private final int maxItemSize;
  private final int readChunkSize;

  public SectionReader(Path path, int maxFileSize, int maxItemSize, int readChunkSize) {
    this.path = path;
    this.maxFileSize = maxFileSize;
    this.maxItemSize = maxItemSize;
    this.readChunkSize = readChunkSize;
  }

  public Iterator<SectionItem> stream(int id) {
    return null;
  }
}
