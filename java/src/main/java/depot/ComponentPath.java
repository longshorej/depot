package depot;

import java.nio.file.Path;

class ComponentPath {
  final Path directory;
  final Path file;

  ComponentPath(final Path directory, final Path file) {
    this.directory = directory;
    this.file = file;
  }
}
