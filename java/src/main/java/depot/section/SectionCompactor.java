package depot.section;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;

public class SectionCompactor {
  private final int maxFileSize;
  private final Path path;

  public SectionCompactor(Path path, int maxFileSize) {
    this.maxFileSize = maxFileSize;
    this.path = path;
  }

  /**
   * Rewrite the section represented by this compactor, including only elements that `filter`
   * returns true for, writing it to the provided destination.
   *
   * <p>To do this, a temporary file is created and all included items are written to it. Markers
   * are interspersed for previous offsets to remain accurate. Once fully written, it is renamed to
   * the destination.
   *
   * <p>Sections can only be compacted if they are full.
   */
  public void compact(Function<SectionItem, Boolean> filter, Path dest) throws IOException {
    SectionStreamer streamer = new SectionStreamer(path, maxFileSize);
    SectionWriter writer = new SectionWriter(dest, maxFileSize);
    SectionEntry entry;
    int bytesRemoved = 0;

    do {
      entry = streamer.next();

      if (entry.eof && !entry.absoluteEof) {
        throw new IOException("Must be eof");
      }

      bytesRemoved += entry.removed;

      boolean appendItem = false;

      if (entry.item != null) {
        if (filter.apply(entry.item)) {
          appendItem = true;
        } else {
          bytesRemoved += entry.item.data.remaining();
        }
      }

      if (bytesRemoved > 0 && (appendItem || entry.eof)) {
        writer.appendRemoved(bytesRemoved);
        bytesRemoved = 0;
      }

      if (appendItem) {
        // @TODO verify truncated... i think this logic is okay
        writer.append(entry.item.data);
      }
    } while (!entry.eof);
  }
}
