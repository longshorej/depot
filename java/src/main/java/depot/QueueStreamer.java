package depot;

import depot.section.SectionEntry;
import depot.section.SectionStreamer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public class QueueStreamer {
  private final int maxFileSize;
  private final Path root;

  private Component component;
  private SectionStreamer sectionStreamer;
  private int sectionId;
  private boolean knownEof;
  private IOException exception;
  private QueueItem queueItem;

  QueueStreamer(final Path root, final long sectionId) {
    this.maxFileSize = 2147287039;
    this.root = root;
    this.knownEof = false;
    this.exception = null;
    this.queueItem = null;

    if (sectionId != -1) {
      final Map.Entry<Component, Integer> componentWithId = Component.decodeId(sectionId);
      this.component = componentWithId.getKey();
      this.sectionId = componentWithId.getValue();
    } else {
      this.component = new Component();
      this.sectionId = -1;
    }
  }

  public void advance(final boolean includeTruncated) {
    while (true) {
      if (knownEof) {
        Component nextComponent = component.next();

        if (nextComponent != null) {
          component = nextComponent;
          knownEof = false;
          sectionStreamer = null;
          sectionId = -1;
        } else {
          return;
        }
      }

      if (sectionStreamer == null) {
        // We haven't opened the next section yet, so attempt to.
        // If it doesn't exist, we do nothing. If it does, attempt
        // to open the file. If that fails, which should be rare,
        // store the error.
        ComponentPath componentPath = component.path(root);

        if (!Files.notExists(componentPath.file)) {
          // a note on the double negative: Files.exists returns false when there's an I/O error,
          // whereas we want to propagate such an error. Thus, always attempt to open the section
          // if we cannot guarantee that the file does not exist.
          try {
            sectionStreamer = new SectionStreamer(componentPath.file, maxFileSize, sectionId);
          } catch (IOException e) {
            exception = e;
            return;
          }
        }
      }

      if (sectionStreamer != null) {
        try {
          // @TODO ensure this is never null
          SectionEntry nextEntry = sectionStreamer.next();

          knownEof = nextEntry.absoluteEof;

          if (nextEntry.item != null) {
            sectionId = nextEntry.item.id;

            if (!nextEntry.item.truncated || includeTruncated) {
              exception = null;
              queueItem = new QueueItem(component.encodeId(nextEntry.item.id), nextEntry.item.data);
              return;
            }
          } else if (nextEntry.eof && !nextEntry.absoluteEof) {
            exception = null;
            queueItem = null;
            return;
          }
        } catch (IOException e) {
          exception = e;
          queueItem = null;
          return;
        }
      }
    }
  }

  public Optional<QueueItem> current() throws IOException {
    if (queueItem != null) {
      return Optional.of(queueItem);
    } else if (exception != null) {
      throw exception;
    } else {
      return Optional.empty();
    }
  }

  public Optional<QueueItem> next() throws IOException {
    advance(false);

    return current();
  }
}
