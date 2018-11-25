package depot.section;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class SectionWriter {
  private final SeekableByteChannel channel;
  private final ByteBuffer buffer;
  private final int maxFileSize;

  private int lastId;
  private int position;

  public SectionWriter(Path path, int maxFileSize) throws IOException {
    this.buffer = ByteBuffer.allocate(16384);
    this.channel = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    this.maxFileSize = maxFileSize;

    long size = channel.size();

    if (size < 4) {
      // minimum item size is 3, and all sections must have an indicator item
      // plus the item termination byte
      // typically, the section will be 0 bytes in this case, but we could have
      // crashed while initializing a file

      buffer.clear();
      buffer.put(
          new byte[] {SectionItem.TYPE_RAW, 0, 1, SectionItem.TYPE_RAW, Section.MARKER_SEPARATOR});
      buffer.flip();
      writeExact();
      buffer.clear();

      size = channel.size();
    }

    // @TODO validate truncation

    channel.position(size);
  }

  public void appendRemoved(int bytesRemoved) throws IOException {
    throw new IOException("not implemented yet");
  }

  public void append(ByteBuffer buffer) throws IOException {
    throw new IOException("not implemented yet");
  }

  public void append(byte[] data) throws IOException {
    if (data.length > Section.MAX_ITEM_SIZE) {
      throw new IOException("item exceeds max item size");
    }

    if (isFull()) {
      throw new IOException("section is full");
    }

    final int nextId = position;

    buffer.put(SectionItem.TYPE_RAW);
    buffer.put((byte) (data.length >> 8));
    buffer.put((byte) (data.length << 8 >> 8));
    position += 3;

    for (int i = 0; i < data.length; i++) {
      byte b = data[i];

      switch (b) {
        case Section.MARKER_ESCAPE:
          buffer.put(Section.MARKER_ESCAPE);
          buffer.put(Section.MARKER_ESCAPE);
          position += 2;
          break;

        case Section.MARKER_SEPARATOR:
          buffer.put(Section.MARKER_ESCAPE);
          buffer.put(Section.MARKER_SEPARATOR_REMAP);
          position += 2;
          break;

        case Section.MARKER_FAIL:
          buffer.put(Section.MARKER_ESCAPE);
          buffer.put(Section.MARKER_FAIL_REMAP);
          position += 2;
          break;

        default:
          buffer.put(b);
          position++;
      }
    }

    buffer.put(Section.MARKER_SEPARATOR);
    position++;

    if (buffer.position() > 8192) {
      sync();
    }

    lastId = nextId;

    if (isFull()) {
      sync();
    }
  }

  private void writeExact() throws IOException {
    int n = 0;
    int l = buffer.limit();
    int r;
    while (n != l) {
      r = channel.write(buffer);
      //  System.out.println("r = " + r + ", n = " + n + ", l = " + l);
      n += r;
    }
  }

  public boolean isEmpty() {
    throw new RuntimeException("not impld yet");
  }

  public boolean isFull() {
    return position >= maxFileSize;
  }

  public void sync() throws IOException {
    buffer.flip();
    writeExact();
    buffer.clear();
  }
}
