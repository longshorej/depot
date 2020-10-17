package depot.section;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Main mechanism for reading items from a section. Use `next` to advance the state of the streamer
 * and return the next item (or null).
 */
public class SectionStreamer {
  private boolean alwaysFail;
  private final SeekableByteChannel channel;
  private byte[] itemBuf;
  private ByteBuffer itemBuffer;
  private int itemLen;
  private int itemStart;
  private int maxFileSize;
  private SectionEntry currentEntry;
  private IOException currentThrowable;
  private int position;
  private int id;
  private byte sectionType;

  private SectionEntry currentResumeEntry;

  /**
   * Creates a new `SectionStreamer` which can read items from the provided file.
   *
   * @param path A path on disk for this section
   * @param maxFileSize A limit to the size of the section. This must match the parameters its
   *     writer used.
   * @throws IOException if there's an I/O error
   */
  public SectionStreamer(final Path path, final int maxFileSize) throws IOException {
    this(path, maxFileSize, -1);
  }

  /**
   * Creates a new `SectionStreamer` which can read items from the provided file.
   *
   * @param path A path on disk for this section
   * @param maxFileSize A limit to the size of the section. This must match the parameters its
   *     writer used.
   * @param id Resume reading from this item (identified by id, also known as an offset).
   * @throws IOException if there's an I/O error
   */
  public SectionStreamer(final Path path, final int maxFileSize, final int id) throws IOException {
    this.alwaysFail = false;
    this.channel = Files.newByteChannel(path, StandardOpenOption.READ);
    this.itemBuf = new byte[Section.MAX_ITEM_SIZE];
    this.itemBuffer = ByteBuffer.wrap(this.itemBuf);
    this.maxFileSize = maxFileSize;

    this.itemStart = 0;
    this.itemLen = 0;
    this.currentEntry = null;
    this.currentResumeEntry = null;
    this.currentThrowable = null;
    this.position = 0;
    this.id = id;
    this.sectionType = -1;

    readSectionType();
  }

  public void seek(final int id) throws IOException {
    channel.position(0L);

    this.itemStart = 0;
    this.itemLen = 0;
    this.currentEntry = null;
    this.currentResumeEntry = null;
    this.currentThrowable = null;
    this.position = 0;
    this.id = id;
    this.sectionType = -1;

    readSectionType();
  }

  private void readSectionType() throws IOException {
    itemStart = 0;

    advance();

    if (currentEntry != null
        && currentEntry.item != null
        && (currentEntry.item.data.hasRemaining() || currentEntry.item.truncated)) {
      sectionType = currentEntry.item.data.get();

      if (currentEntry.item.truncated) {
        // it's possible that the marker item was written but
        // there was a crash before it was fully written
        // in this case, the writer will marker it as such
        // upon next write as truncated.
        //
        // because compacted sections are not subject to this
        // -- they must be fully written -- we can assume that
        // if the marker item is truncated, then this is a raw
        // section
        sectionType = SectionItem.TYPE_RAW;
      }

      if (id >= 0) {
        switch (sectionType) {
          case SectionItem.TYPE_RAW:
            channel.position(id);
            position = id;
            itemStart = 0;
            itemLen = 0;
            break;

          case SectionItem.TYPE_REMOVED:
            SectionEntry entry;

            // for compacted sections, we have to take a performance hit for resuming from offsets
            // this means scanning the file until we find our id. note that item can be null
            // if it is part of a sequence of items that were removed, so we skip those as well.
            while (!(entry = next()).eof && (entry.item == null || entry.item.id < id)) {}

            this.currentResumeEntry = entry;

            break;

          default:
            throw new IOException("Invalid section type byte: " + sectionType);
        }
      }
    } else if (currentThrowable != null) {
      throw currentThrowable;
    }
  }

  /**
   * Advances to the next item and returns it. If at the end of the section, this returns null. If
   * at EOF (having returned null), if another item is appended, the next call to `next` will return
   * that item, i.e. it is valid to call `next` after receiving null.
   *
   * @return the next item, or null if at the end of the section.
   * @throws IOException if there's an I/O error
   */
  public SectionEntry next() throws IOException {
    if (sectionType == -1) {
      readSectionType();

      if (sectionType == -1) {
        return new SectionEntry(null, false, true, 0);
      }
    }

    if (currentResumeEntry != null) {
      currentEntry = currentResumeEntry;
      currentThrowable = null;
      currentResumeEntry = null;
    } else {
      advance();
    }

    if (currentEntry != null) {
      return currentEntry;
    } else if (currentThrowable != null) {
      throw currentThrowable;
    } else {
      // this should be unreachable..
      throw new IllegalStateException("next() would return null; this is a bug");
    }
  }

  private void advance() {
    if (alwaysFail) {
      currentEntry = null;
      currentThrowable = new IOException("a previous error has halted further execution");
      return;
    }

    while (true) {
      if (itemLen - itemStart >= 4) {
        System.out.println("ub=" + itemBuf[itemStart + 1]);
        System.out.println("lb=" + itemBuf[itemStart + 2]);
        final int dataSize =
            ((int) itemBuf[itemStart + 1] << 8) | (byte) (itemBuf[itemStart + 2] << 8 >> 8);
        System.out.println("dataSize=" + dataSize);

        // we use the expected end as a hint for where the EOF byte is
        // but note that it could be wrong when e.g. truncated, so if
        // our expectations aren't met we fall back to a scan.
        final int expectedEnd = itemStart + 2 + dataSize + 1;
        final int startScanFrom =
            expectedEnd < itemLen && itemBuf[expectedEnd] == Section.MARKER_SEPARATOR
                ? expectedEnd
                : itemStart + 3; // 3 is the type byte plus two size bytes

        for (int i = startScanFrom; i < itemLen; i++) {

          if (itemBuf[i] == Section.MARKER_SEPARATOR) {
            final int nextPosition = position + (i - itemStart) + 1;
            final byte type = itemBuf[itemStart];

            boolean needsDecode = false;
            switch (type) {
              case SectionItem.TYPE_ENCODED:
                needsDecode = true;
              case SectionItem.TYPE_RAW:
                final boolean truncated = itemBuf[i - 1] == Section.MARKER_FAIL || i != expectedEnd;

                if (needsDecode && !truncated) {
                  boolean escaped = false;
                  int shifted = 0;

                  for (int j = itemStart; j < i; j++) {
                    final byte b2 = itemBuf[j];

                    if (escaped) {
                      escaped = false;

                      if (b2 == Section.MARKER_FAIL_REMAP) {
                        itemBuf[j - shifted] = Section.MARKER_FAIL;
                      } else if (b2 == Section.MARKER_SEPARATOR_REMAP) {
                        itemBuf[j - shifted] = Section.MARKER_SEPARATOR;
                      } else if (b2 == Section.MARKER_ESCAPE) {
                        itemBuf[j - shifted] = Section.MARKER_ESCAPE;
                      } else {
                        alwaysFail = true;

                        currentThrowable =
                            new IOException(
                                "cannot parse file, invalid byte " + b2 + " after escape");

                        return;
                      }
                    } else if (b2 == Section.MARKER_ESCAPE) {
                      escaped = true;
                      shifted++;
                    } else if (shifted > 0) {
                      itemBuf[j - shifted] = b2;
                    }
                  }
                }

                final SectionItem item =
                    new SectionItem(
                        type,
                        position,
                        ByteBuffer.wrap(itemBuf, itemStart + 3, i - itemStart - 3),
                        truncated);

                itemStart = i + 1;
                position = nextPosition;
                final boolean absoluteEof = nextPosition > maxFileSize;
                currentEntry = new SectionEntry(item, absoluteEof, absoluteEof, 0);
                currentThrowable = null;
                return;

              case SectionItem.TYPE_REMOVED:
                // @TODO assert record separator, fail as that indicates file corruption

                final int bytesRemoved =
                    ((0xFF & itemBuf[itemStart + 1]) << 24)
                        | ((0xFF & itemBuf[itemStart + 2]) << 16)
                        | ((0xFF & itemBuf[itemStart + 3]) << 8)
                        | (0xFF & itemBuf[itemStart + 4]);

                itemStart += 6; // 1 item type, 4 length, 1 terminator
                position += bytesRemoved;

                final boolean knownEof = position > maxFileSize;

                currentEntry = new SectionEntry(null, knownEof, knownEof, bytesRemoved);
                currentThrowable = null;
                return;

              default:
                alwaysFail = true;
                currentThrowable = new IOException("cannot parse file, invalid type " + type);

                return;
            }
          }
        }
      }

      // we don't have enough data to process another item, so shift what's remaining over
      // and continue.

      int nextItemLen = 0;
      for (int j = itemStart; j < itemLen; j++) {
        itemBuf[nextItemLen] = itemBuf[j];
        nextItemLen++;
      }

      itemStart = 0;
      itemLen = nextItemLen;
      itemBuffer.position(itemLen);

      try {
        final int read = channel.read(itemBuffer);

        if (read < 0) {
          currentEntry = new SectionEntry(null, false, true, 0);
          currentThrowable = itemLen < 1 ? null : new IOException("maximum item size exceeded");
          return;
        } else {
          itemLen += read;
        }
      } catch (IOException e) {
        currentEntry = null;
        currentThrowable = e;
        return;
      }
    }
  }
}
