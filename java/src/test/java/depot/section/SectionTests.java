package depot.section;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.*;
import static org.junit.Assert.*;

public class SectionTests {
  /**
   * This test writes 300 items to a section and removes the middle ~250. It then tests that
   * resuming from the compacted offsets behaves as expected, i.e. behavior is unchanged.
   */
  @Test
  public void testCompaction() throws IOException {
    int maxFileSize = 1024 * 1024 * 8;
    Path path = Files.createTempFile("depot-tests", "dpo");
    Path compactedPath = Files.createTempFile("depot-tests", "dpo");
    Path recompactedPath = Files.createTempFile("depot-tests", "dpo");

    try {
      SectionWriter writer = new SectionWriter(path, maxFileSize);

      for (int i = 1; i <= 300; i++) {
        writer.append(("this is test #" + i).getBytes(StandardCharsets.UTF_8));
      }

      writer.sync();

      SectionStreamer streamer = new SectionStreamer(path, maxFileSize);

      SectionEntry entry;

      int i = 0;
      SectionItem entry1 = null;
      SectionItem entry250 = null;
      SectionItem entry260 = null;

      do {
        entry = streamer.next();

        if (entry.item != null) {
          i++;

          if (i == 260) {
            assertEquals(entry.item.toDecodedString(), "this is test #260");
            entry260 = entry.item;
          } else if (i == 250) {
            assertEquals(entry.item.toDecodedString(), "this is test #250");
            entry250 = entry.item;
          } else if (i == 1) {
            assertEquals(entry.item.toDecodedString(), "this is test #1");
            entry1 = entry.item;
          }
        }
      } while (!entry.eof);

      assertEquals(300, i);
      assertNotNull(entry1);
      assertNotNull(entry250);
      assertNotNull(entry260);

      // test seeking to an offset
      streamer.seek(entry250.id);
      SectionEntry item250 = streamer.next();
      assertEquals(entry250, item250.item);

      // compact a file
      SectionCompactor sectionCompactor = new SectionCompactor(path, maxFileSize);
      final int entry1Id = entry1.id;
      final int entry250Id = entry250.id;
      sectionCompactor.compact(
          i2 -> i2.id == entry1Id || i2.id >= entry250Id, compactedPath, false);

      // test resuming from a later offset
      SectionStreamer compactedStreamer1 =
          new SectionStreamer(compactedPath, maxFileSize, item250.item.id);
      assertEquals(entry250.id, compactedStreamer1.next().item.id);
      for (int j = 251; j <= 300; j++) {
        assertEquals("this is test #" + j, compactedStreamer1.next().item.toDecodedString());
      }

      // test resuming from an earlier offset
      SectionStreamer compactedStreamer2 =
          new SectionStreamer(compactedPath, maxFileSize, entry1Id);
      assertEquals(entry1Id, compactedStreamer2.next().item.id);

      SectionEntry removedEntry = compactedStreamer2.next();
      assertEquals(5102, removedEntry.removed);
      assertNull(removedEntry.item);

      for (int j = 250; j <= 300; j++) {
        assertEquals("this is test #" + j, compactedStreamer2.next().item.toDecodedString());
      }

      // test resuming with no offset
      SectionStreamer compactedStreamer3 = new SectionStreamer(compactedPath, maxFileSize);
      assertEquals(entry1Id, compactedStreamer3.next().item.id);
      assertNull(compactedStreamer3.next().item);
      for (int j = 250; j <= 300; j++) {
        assertEquals("this is test #" + j, compactedStreamer3.next().item.toDecodedString());
      }

      // now let's remove 250-260, and test that the consecutive
      // removals get merged
      final int entry260Id = entry260.id;
      SectionCompactor sectionRecompactor = new SectionCompactor(compactedPath, maxFileSize);
      sectionRecompactor.compact(i2 -> i2.id >= entry260Id, recompactedPath, false);

      SectionStreamer recompactedStreamer = new SectionStreamer(recompactedPath, maxFileSize);
      SectionEntry recompactedRemovedEntry = recompactedStreamer.next();
      assertEquals(5331, recompactedRemovedEntry.removed);
      assertNull(recompactedRemovedEntry.item);
      for (int j = 260; j <= 300; j++) {
        assertEquals("this is test #" + j, recompactedStreamer.next().item.toDecodedString());
      }
    } finally {
      Files.delete(path);
      Files.delete(compactedPath);
      Files.delete(recompactedPath);
    }
  }
}
