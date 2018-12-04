package depot;

import depot.section.SectionWriter;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public class QueueWriter implements Closeable {
  private static final int readChunkSize = 8192;
  private static final int writeChunkSize = 8192;

  private final int maxFileSize;
  private final int maxItemSize;
  private final Path root;

  private Component component;
  private SectionWriter section;

  public QueueWriter(final Path root) {
    this.maxFileSize = 2147287039;
    this.maxItemSize = 8192;
    this.root = root;
  }

  public void append(final byte[] data) throws IOException {
    append(ByteBuffer.wrap(data));
  }

  public void append(final ByteBuffer data) throws IOException {
    initialize();

    if (section.isFull()) {
      advance();
    }

    section.append(data);
  }

  @Override
  public void close() throws IOException {
    if (section != null) {
      section.close();
      component = null;
      section = null;
    }
  }

  public void sync() throws IOException {
    if (section != null) {
      section.sync();
    }
  }

  private void advance() throws IOException {
    initialize();

    section.sync();

    final Component nextComponent =
        component.next().orElseThrow(() -> new IllegalStateException("queue is full"));

    final ComponentPath nextComponentPath = nextComponent.path(root);

    Files.createDirectories(nextComponentPath.directory);

    section = new SectionWriter(nextComponentPath.file, maxFileSize);
    component = nextComponent;
  }

  private void initialize() throws IOException {
    if (section == null) {
      final DepotPath c0 = DepotPath.latestDir(root);
      final DepotPath c1 = DepotPath.latestDir(c0.path);
      final DepotPath c2 = DepotPath.latestDir(c1.path);
      final DepotPath c3 = DepotPath.latestFile(c2.path);

      component = new Component(c0.id, c1.id, c2.id, c3.id);
      section = new SectionWriter(c3.path, maxFileSize);
    }
  }
}
