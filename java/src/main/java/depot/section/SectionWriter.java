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

  public SectionWriter(final Path path, final int maxFileSize) throws IOException {
    this(path, maxFileSize, SectionItem.TYPE_RAW);
  }

  SectionWriter(final Path path, final int maxFileSize, final byte sectionType) throws IOException {
    this.buffer = ByteBuffer.allocate(16384); // @TODO this should be 8k
    this.channel = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    this.maxFileSize = maxFileSize;

    long size = channel.size();

    if (size < 4) {
      // minimum item size is 3, and all sections must have an indicator item
      // plus the item termination byte
      // typically, the section will be 0 bytes in this case, but we could have
      // crashed while initializing a file

      buffer.clear();
      buffer.put(new byte[] {SectionItem.TYPE_RAW, 0, 1, sectionType, Section.MARKER_SEPARATOR});
      buffer.flip();
      writeExact();
      buffer.clear();

      size = channel.size();
    }

    channel.position(size);
  }

  public void append(final byte[] data) throws IOException {
    append(ByteBuffer.wrap(data));
  }

  public void append(final ByteBuffer data) throws IOException {
    final int length = data.remaining();
    final int start = data.position();

    if (length > Section.MAX_ITEM_SIZE) {
      throw new IOException("item exceeds max item size");
    }

    if (isFull()) {
      throw new IOException("section is full");
    }

    final int nextId = position;

    boolean encoded = false;
    for (int i = 0; i < length; i++) {
      byte b = data.get(start + i);

      if (b == Section.MARKER_ESCAPE || b == Section.MARKER_SEPARATOR || b == Section.MARKER_FAIL) {
        encoded = true;

        break;
      }
    }

    write(encoded ? SectionItem.TYPE_ENCODED : SectionItem.TYPE_RAW);
    write((byte) (length >> 8));
    write((byte) (length));

    for (int i = 0; i < length; i++) {
      final byte b = data.get(start + i);

      if (encoded) {
        switch (b) {
          case Section.MARKER_ESCAPE:
            write(Section.MARKER_ESCAPE);
            write(Section.MARKER_ESCAPE);
            encoded = true;
            break;

          case Section.MARKER_SEPARATOR:
            write(Section.MARKER_ESCAPE);
            write(Section.MARKER_SEPARATOR_REMAP);
            encoded = true;
            break;

          case Section.MARKER_FAIL:
            write(Section.MARKER_ESCAPE);
            write(Section.MARKER_FAIL_REMAP);
            encoded = true;
            break;

          default:
            write(b);
        }
      } else {
        write(b);
      }
    }

    write(Section.MARKER_SEPARATOR);

    if (isFull()) {
      sync();
    }

    lastId = nextId;
  }

  void appendRemoved(final int bytesRemoved) throws IOException {
    if (isFull()) {
      throw new IOException("section is full");
    }

    final int nextId = position;

    write(SectionItem.TYPE_REMOVED);
    write((byte) (bytesRemoved >> 24));
    write((byte) (bytesRemoved >> 16));
    write((byte) (bytesRemoved >> 8));
    write((byte) (bytesRemoved));
    write(Section.MARKER_SEPARATOR);

    lastId = nextId;

    if (isFull()) {
      sync();
    }
  }

  private void write(final byte b) throws IOException {
    buffer.put(b);
    position++;

    if (buffer.position() == buffer.capacity()) {
      sync();
    }
  }

  private void writeExact() throws IOException {
    int n = 0;
    final int l = buffer.limit();
    int r;
    while (n != l) {
      r = channel.write(buffer);
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
