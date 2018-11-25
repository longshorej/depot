package depot;

import depot.section.SectionEntry;
import depot.section.SectionItem;
import depot.section.SectionStreamer;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import depot.section.SectionWriter;
import org.junit.*;
import static org.junit.Assert.*;

public class QueueTests {
  @Test
  public void test2() throws IOException {
    if (true) return;

    SectionWriter writer =
        new SectionWriter(Paths.get("/home/longshorej/test-jdepot"), 1024 * 1024 * 1024);
    long t0 = System.nanoTime();

    int i;

    for (i = 0; i < 20000000; i++) {
      writer.append("hello world".getBytes(StandardCharsets.UTF_8));
    }

    writer.sync();

    long t1 = System.nanoTime();

    long tt = (t1 - t0) / 1000 / 1000;

    System.out.println("wrote " + i + " items in " + tt + "ms");
  }

  @Test
  public void test() throws IOException {
    assertTrue(true);

    // if (true) return;

    SectionStreamer iterator =
        new SectionStreamer(Paths.get("/home/longshorej/test-jdepot"), 1024 * 1024 * 1024, -1);

    long t0 = System.nanoTime();

    int i = 0;

    SectionEntry entry;

    while (!(entry = iterator.next()).eof) {
      // String decoded = entry.item.dataAsString();

      // System.out.println(decoded);

      i++;
    }

    long t1 = System.nanoTime();

    long tt = (t1 - t0) / 1000 / 1000;

    System.out.println("read " + i + " items in " + tt + "ms");
  }
}
