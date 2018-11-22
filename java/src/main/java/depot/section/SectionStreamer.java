package depot.section;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

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
  private SectionItem currentItem;
  private IOException currentThrowable;
  private int position;

  /**
   * Creates a new `SectionStreamer` which can read items from the provided file.
   *
   * @param path A path on disk for this section
   * @param maxFileSize A limit to the size of the section. This must match the parameters its
   *     writer used.
   * @param id Resume reading from this item (identified by id, also known as an offset). The first
   *     item of a section always has id 0.
   * @throws IOException if there's an I/O error
   */
  public SectionStreamer(Path path, int maxFileSize, int id) throws IOException {
    if (id < 0) {
      throw new IllegalArgumentException("id cannot be negative");
    }

    this.alwaysFail = false;
    this.channel = Files.newByteChannel(path);
    this.itemBuf = new byte[Section.MAX_ITEM_SIZE];
    this.itemBuffer = ByteBuffer.wrap(this.itemBuf);
    this.itemStart = 0;
    this.itemLen = 0;
    this.maxFileSize = maxFileSize;
    this.currentItem = null;
    this.currentThrowable = null;
    this.position = id;
  }

  /**
   * Advances to the next item and returns it. If at the end of the section, this returns null. If
   * at EOF (having returned null), if another item is appended, the next call to `next` will return
   * that item, i.e. it is valid to call `next` after receiving null.
   *
   * @return the next item, or null if at the end of the section.
   * @throws IOException if there's an I/O error
   */
  public SectionItem next() throws IOException {
    advance();

    if (currentItem != null) {
      return currentItem;
    } else if (currentThrowable != null) {
      throw currentThrowable;
    } else {
      return null;
    }
  }

  private void advance() {
    if (alwaysFail) {
      currentItem = null;
      currentThrowable = new IOException("a previous error has halted further execution");
      return;
    }

    while (true) {
      boolean needDecode = false;
      byte lastByte = 0;

      for (int i = itemStart; i < itemLen; i++) {
        byte b = itemBuf[i];

        if (b == Section.MARKER_SEPARATOR) {
          int nextPosition = position + (i - itemStart) + 1;
          boolean truncated = lastByte == Section.MARKER_FAIL;

          boolean escaped = false;
          int shifted = 0;

          if (needDecode && !truncated) {
            for (int j = itemStart; j < i; j++) {
              byte b2 = itemBuf[j];

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
                      new IOException("cannot parse file, invalid byte " + b2 + " after escape");

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

          SectionItem item =
              new SectionItem(
                  position,
                  ByteBuffer.wrap(itemBuf, itemStart, i - itemStart),
                  nextPosition > maxFileSize,
                  truncated);

          itemStart = i + 1;
          position = nextPosition;

          currentItem = item;
          return;
        } else if (b == Section.MARKER_ESCAPE) {
          needDecode = true;
        }

        lastByte = b;
      }

      int nextItemLen = 0;
      for (int j = itemStart; j < itemLen; j++) {
        itemBuf[nextItemLen] = itemBuf[j];
        nextItemLen++;
      }

      itemStart = 0;
      itemLen = nextItemLen;
      itemBuffer.position(itemLen);

      try {
        int read = channel.read(itemBuffer);

        itemLen += read;

        if (read < 0) {
          currentItem = null;
          currentThrowable = itemLen < 1 ? null : new IOException("maximum item size exceeded");
          return;
        }
      } catch (IOException e) {
        currentItem = null;
        currentThrowable = e;
        return;
      }
    }
  }
}
