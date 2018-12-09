package depot;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class Queue implements Closeable {
  private final Path root;
  private QueueWriter queueWriter;

  public Queue(final Path root) {
    this.root = root;
    this.queueWriter = null;
  }

  public QueueStreamer stream() {
    return new QueueStreamer(root, -1);
  }

  public QueueStreamer stream(long id) {
    return new QueueStreamer(root, id);
  }

  public void append(final byte[] data) throws IOException {
    append(ByteBuffer.wrap(data));
  }

  public void append(final ByteBuffer data) throws IOException {
    if (queueWriter == null) {
      queueWriter = new QueueWriter(root);
    }

    queueWriter.append(data);
  }

  @Override
  public void close() throws IOException {
    if (queueWriter != null) {
      queueWriter.close();
      queueWriter = null;
    }
  }
}
