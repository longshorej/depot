package depot;

import depot.section.SectionCompactor;
import depot.section.SectionWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.Function;

class QueueCompactor {
  private final int maxFileSize;
  private final Path root;

  QueueCompactor(final Path root, final int maxFileSize) {
    this.maxFileSize = maxFileSize;
    this.root = root;
  }

  public void compact(final Function<QueueItem, Boolean> filter) throws IOException {
    Component component = new Component();

    while (true) {
      final Component currentComponent = component;
      final ComponentPath componentPath = component.path(root);
      final SectionCompactor compactor = new SectionCompactor(componentPath.file, maxFileSize);

      if (Files.notExists(componentPath.file)) {
        return;
      }

      final SectionWriter writer = new SectionWriter(componentPath.file, maxFileSize);

      if (!writer.isFull()) {
        writer.close();
        return;
      } else {
        writer.close();
      }

      Path compactedSection = Files.createTempFile(root, "depot", "dpoc");

      compactor.compact(
          item -> {
            QueueItem queueItem = new QueueItem(currentComponent.encodeId(item.id), item.data);

            return filter.apply(queueItem);
          },
          compactedSection);

      // @TODO support fat32

      Files.move(compactedSection, componentPath.file, StandardCopyOption.ATOMIC_MOVE);

      component = component.next();
    }
  }
}
