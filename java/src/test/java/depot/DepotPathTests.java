package depot;

import org.junit.*;
import static org.junit.Assert.*;

public class DepotPathTests {
  @Test
  public void extractNumberWorks() {
    assertEquals(-1, DepotPath.extractNumber("abc"));
    assertEquals(0, DepotPath.extractNumber("d0"));
    assertEquals(1, DepotPath.extractNumber("d1"));
    assertEquals(999, DepotPath.extractNumber("d999"));
    assertEquals(32767, DepotPath.extractNumber("d32767"));
    assertEquals(-1, DepotPath.extractNumber("d32768"));
    assertEquals(-1, DepotPath.extractNumber("d32769"));
    assertEquals(-1, DepotPath.extractNumber("d-2"));
  }
}
