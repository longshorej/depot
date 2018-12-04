package depot;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

class DepotPath {
  static DepotPath latestDir(final Path base) throws IOException {
    // @TODO ensure that it's a directory
    final DepotPath l = latest(base);

    if (l == null) {
      final Path dir = base.resolve("d0");
      Files.createDirectories(dir);
      return new DepotPath(dir, (short) 0);
    }

    return l;
  }

  static DepotPath latestFile(final Path base) throws IOException {
    // @TODO ensure that it's a file
    final DepotPath l = latest(base);

    if (l == null) {
      final Path dir = base.resolve("d0.dpo");
      return new DepotPath(dir, (short) 0);
    }

    return l;
  }

  static DepotPath latest(final Path base) throws IOException {
    DepotPath path = null;
    short num = -1;

    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(base)) {
      for (Path nextPath : directoryStream) {
        final short nextNum = extractNumber(nextPath.toString());

        if (nextNum > num) {
          path = new DepotPath(nextPath, nextNum);
          num = nextNum;
        }
      }

      return path;
    }
  }

  private static short extractNumber(final String name) {
    if (name.startsWith("d")) {
      final String numericData =
          name.substring(1, name.endsWith(".dpo") ? name.length() - 4 : name.length());

      if (numericData.chars().allMatch(c -> c >= 0 && c <= 9)) {
        try {
          final int value = Integer.parseInt(numericData);

          if (value <= Short.MAX_VALUE) {
            return (short) value;
          }
        } catch (NumberFormatException ignore) {
        }
      }
    }

    return -1;
  }

  final Path path;
  final short id;

  DepotPath(final Path path, final short id) {
    this.path = path;
    this.id = id;
  }
}
