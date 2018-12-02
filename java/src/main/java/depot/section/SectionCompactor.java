package depot.section;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.Function;

/**
 * A `SectionCompactor` provides methods for copying sections into new files and removing some of
 * their items.
 */
public class SectionCompactor {
  private final int maxFileSize;
  private final Path path;

  public SectionCompactor(final Path path, final int maxFileSize) {
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
   *
   * <p>When compacting a section, adjacent removals are merged.
   */
  public void compact(final Function<SectionItem, Boolean> filter, final Path dest)
      throws IOException {
    compact(filter, dest, true);
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
  void compact(final Function<SectionItem, Boolean> filter, final Path dest, final boolean onlyFull)
      throws IOException {
    final Path tempDest = Files.createTempFile(dest.getParent(), "depot", "dpo");
    final SectionStreamer streamer = new SectionStreamer(path, maxFileSize);
    final SectionWriter writer = new SectionWriter(tempDest, maxFileSize, SectionItem.TYPE_REMOVED);
    SectionEntry entry;
    int bytesRemoved = 0;

    do {
      entry = streamer.next();

      if (entry.eof && onlyFull && !entry.absoluteEof) {
        throw new IOException("Must be eof");
      }

      bytesRemoved += entry.removed;

      boolean appendItem = false;

      if (entry.item != null) {
        if (filter.apply(entry.item)) {
          appendItem = true;
        } else {
          // 4 bytes is the minimum overhead: 1 for the type, 2 for the length, 1 for the separator
          bytesRemoved += 4 + entry.item.data.remaining();
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

    writer.sync();

    Files.move(tempDest, dest, StandardCopyOption.ATOMIC_MOVE);
  }
}
