package depot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

public class QueueTests {
  @Test
  public void testQueue() throws IOException {
    Path path = Files.createTempDirectory("depot-tests");

    try {
      try (QueueWriter writer = new QueueWriter(path, 2147287039)) {
        writer.append(ByteBuffer.wrap("Hello World!".getBytes(StandardCharsets.UTF_8)));
        writer.sync();
      }

      try (QueueWriter writer = new QueueWriter(path, 2147287039)) {
        writer.append(ByteBuffer.wrap("Hello World 2!".getBytes(StandardCharsets.UTF_8)));
        writer.sync();
      }

      QueueStreamer streamer = new QueueStreamer(path, -1);

      Optional<QueueItem> nextQueueItem;

      while ((nextQueueItem = streamer.next()).isPresent()) {
        System.out.println(nextQueueItem.get().toDecodedString());
      }

    } finally {
      removeRecursive(path);
    }
  }

  @Test
  public void testQueueRandomData() throws IOException {
    final Path path = Files.createTempDirectory("depot-tests");

    try {
      final Queue queue = new Queue(path, 65536);

      long writtenSum = 0;

      for (int i = 0; i < 65536; i++) {
        final byte[] data = randomData();
        queue.append(data);

        for (byte b : data) {
          writtenSum += b;
        }
      }

      queue.sync();

      try (QueueWriter writer = new QueueWriter(path, 65536)) {
        writer.append(ByteBuffer.wrap("Hello World!".getBytes(StandardCharsets.UTF_8)));
        writer.sync();
      }

      try (QueueWriter writer = new QueueWriter(path, 65536)) {
        writer.append(ByteBuffer.wrap("Hello World 2!".getBytes(StandardCharsets.UTF_8)));
        writer.sync();
      }

      final QueueStreamer queueStreamer = queue.stream();

      Optional<QueueItem> maybeQueueItem;
      long readSum = 0;

      while ((maybeQueueItem = queueStreamer.next()).isPresent()) {
        final OwnedQueueItem item = maybeQueueItem.get().toOwned();

        for (byte b : item.data) {
          readSum += b;
        }
      }

      assertEquals(writtenSum, readSum);

    } finally {
      System.out.println(path);
      // removeRecursive(path);
    }
  }

  private static byte[] randomData() {
    byte[] b = new byte[128];
    // new Random().nextBytes(b);
    for (int i = 0; i < b.length; i++) {
      b[i] = 45;
    }
    return b;
  }

  /**
   * Recursively remove a directory.
   *
   * <p>Credit: https://stackoverflow.com/a/8685959
   */
  private static void removeRecursive(Path path) throws IOException {
    Files.walkFileTree(
        path,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            // try to delete the file anyway, even if its attributes
            // could not be read, since delete-only access is
            // theoretically possible
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            if (exc == null) {
              Files.delete(dir);
              return FileVisitResult.CONTINUE;
            } else {
              // directory iteration failed; propagate exception
              throw exc;
            }
          }
        });
  }
}
