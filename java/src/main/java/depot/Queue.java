package depot;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.Function;

public class Queue implements Closeable {
  private final int maxFileSize;
  private final Path root;
  private QueueWriter queueWriter;

  public Queue(final Path root) {
    this(root, 2147287039);
  }

  Queue(final Path root, final int maxFileSize) {
    this.root = root;
    this.queueWriter = null;
    this.maxFileSize = maxFileSize;
  }

  public QueueStreamer stream() {
    return new QueueStreamer(root, -1);
  }

  public QueueStreamer stream(long id) {
    return new QueueStreamer(root, id);
  }

  public void sync() throws IOException {
    if (queueWriter != null) {
      queueWriter.sync();
    }
  }

  public void append(final byte[] data) throws IOException {
    append(ByteBuffer.wrap(data));
  }

  public void append(final ByteBuffer data) throws IOException {
    if (queueWriter == null) {
      queueWriter = new QueueWriter(root, maxFileSize);
    }

    queueWriter.append(data);
  }

  // @TODO allow compaction to resume from an id
  public void compact(final Function<QueueItem, Boolean> filter) throws IOException {
    QueueCompactor queueCompactor = new QueueCompactor(root, maxFileSize);
    queueCompactor.compact(filter);
  }

  @Override
  public void close() throws IOException {
    if (queueWriter != null) {
      queueWriter.close();
      queueWriter = null;
    }
  }
}
