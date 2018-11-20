package depot.section;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.OptionalInt;

public class SectionIterator implements Iterator<SectionItem> {

  private boolean alwaysFail;
  private final SeekableByteChannel channel;
  private byte[] itemBuf;
  private ByteBuffer itemBuffer;
  private int itemLen;
  private int itemStart;
  private int maxFileSize;
  private SectionItem currentItem;
  private Throwable currentThrowable;
  private int position;

  public SectionIterator(Path path, int maxFileSize, int maxItemSize, int id) throws IOException {
    if (maxItemSize != Section.MAX_ITEM_SIZE) {
      throw new IllegalArgumentException("maxItemSize is not currently configurable");
    }

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

    advance();
  }

  private void advance() {
    if (alwaysFail) {
      currentItem = null;
      currentThrowable = new IllegalStateException("a previous error has halted further execution");
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
                      new IllegalStateException(
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
          currentThrowable =
              itemLen < 1 ? null : new IllegalStateException("maximum item size exceeded");
          return;
        }
      } catch (IOException e) {
        currentItem = null;
        currentThrowable = e;
        return;
      }
    }
  }

  @Override
  public boolean hasNext() {
    // @TODO should "peak" somehow
    return currentItem != null || currentThrowable != null;
  }

  @Override
  public SectionItem next() {
    // @TODO reference to existing
    if (currentItem != null) {
      SectionItem item = currentItem;
      advance();

      return item;
    } else if (currentThrowable != null) {
      Throwable throwable = currentThrowable;
      advance();
      throw new IllegalStateException(throwable);
    } else {
      advance();
      throw new NoSuchElementException();
    }
  }
}
