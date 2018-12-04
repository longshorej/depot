package depot;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import org.junit.*;
import static org.junit.Assert.*;

public class QueueTests {
  @Test
  public void testQueue() throws IOException {
    Path path = Files.createTempDirectory("depot-tests");

    try {
      try (QueueWriter writer = new QueueWriter(path)) {
        writer.append("Hello World!".getBytes(StandardCharsets.UTF_8));
        writer.sync();
      }

      try (QueueWriter writer = new QueueWriter(path)) {
        writer.append("Hello World 2!".getBytes(StandardCharsets.UTF_8));
        writer.sync();
      }

    } finally {
      removeRecursive(path);
      System.out.println(path);
    }
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
