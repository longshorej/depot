package depot.section;

import scalaz.Alpha;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

public class SectionWriter {
  private final SeekableByteChannel channel;
  private int lastId;
  private int position;

  public SectionWriter() {
    thr
  }

  public void append(byte[] data) throws IOException {
    if (data.length > Section.MAX_ITEM_SIZE) {
      throw new IOException("item exceeds max item size");
    }

    if (isFull()) {
      throw new IOException("section is full");
    }

    int nextId = position;

    for (int i = 0; i < data.length; i++) {
      byte b = data[i];

      switch (b) {
        case Section.MARKER_ESCAPE:
          write(Section.MARKER_ESCAPE, Section.MARKER_ESCAPE);
          position += 2;
          break;

        case Section.MARKER_SEPARATOR:
          write(Section.MARKER_ESCAPE, Section.MARKER_SEPARATOR_REMAP);
          position += 2;
          break;

        case Section.MARKER_FAIL:
          write(Section.MARKER_ESCAPE, Section.MARKER_FAIL_REMAP);
          position += 2;
          break;

        default:
          write(b);
          position++;
      }

      write(Section.MARKER_SEPARATOR);
      position++;

      lastId = nextId;

      if (isFull()) {
        sync();
      }
    }
  }

  private void sync() {

  }

  private void write(byte b1) {

  }

  private void write(byte b1, byte b2) {

  }

  public boolean isFull() {

  }
}
