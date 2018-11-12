package depot;

import org.junit.*;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class QueueTests {
  @Test
  public void initialTest() {
    try (Queue section = new Queue("/home/longshorej/testing2")) {
      System.out.println("empty=" + section.isEmpty());
      System.out.println("full=" + section.isFull());
      System.out.println("lastId=" + section.getLastId());

      // timeItWrite(section);
      timeItRead(section);
    }
  }

  private void timeItRead(Queue section) {
    int total = 1000000;
    long t1 = System.currentTimeMillis();

    try (QueueIterator stream = section.stream(-1)) {
      while (stream.hasNext()) {
        QueueItem item = stream.next();

        // System.out.println("#" + item.id + ": " + new String(item.data));
      }
    }

    long t2 = System.currentTimeMillis();
    long ms = t2 - t1;
    long perSecond = (long) (total / (ms * 1.0D / 1000));

    System.out.println("ms taken: " + ms + ", per second: " + perSecond);
  }

  private void timeItWrite(Queue section) {
    int total = 1000000;
    long t1 = System.currentTimeMillis();

    byte[] data = "hello world!".getBytes(StandardCharsets.UTF_8);

    for (int i = 0; i < total; i++) {
      section.append(data);
    }

    section.sync();

    long t2 = System.currentTimeMillis();
    long ms = t2 - t1;
    long perSecond = (long) (total / (ms * 1.0D / 1000));

    System.out.println("ms taken: " + ms + ", per second: " + perSecond);
  }
}
