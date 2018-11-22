package depot;

import depot.section.SectionItem;
import depot.section.SectionStreamer;
import java.io.*;
import java.nio.file.Paths;
import org.junit.*;
import static org.junit.Assert.*;

public class QueueTests {
  @Test
  public void test() throws IOException {
    assertTrue(true);

    SectionStreamer iterator =
        new SectionStreamer(Paths.get("/home/longshorej/testing2/d0/d0/d0/d0"), 1073741824, 0);

    long t0 = System.nanoTime();

    int i = 0;

    SectionItem item;

    while ((item = iterator.next()) != null) {
      String decoded = item.dataAsString();

      i++;
    }

    long t1 = System.nanoTime();

    long tt = (t1 - t0) / 1000 / 1000;

    System.out.println("read " + i + " items in " + tt + "ms");
  }
}
