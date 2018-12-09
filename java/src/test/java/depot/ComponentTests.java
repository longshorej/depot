package depot;

import java.nio.file.Paths;
import java.util.Optional;
import org.junit.*;
import static org.junit.Assert.*;

public class ComponentTests {
  @Test
  public void emptyAndFull() {
    assertEquals(0, new Component().encode());
    assertEquals(0, new Component(0).encode());
    assertEquals(0, new Component((short) 0, (short) 0, (short) 0, (short) 0).encode());

    assertTrue(new Component(0).isEmpty());
    assertFalse(new Component(0).isFull());

    assertFalse(new Component(1000).isEmpty());

    assertTrue(new Component(1999999999).isFull());

    assertEquals(1999999999, new Component(1999999999).encode());
    assertEquals(
        new Component((short) 1, (short) 999, (short) 999, (short) 999), new Component(1999999999));
  }

  @Test(expected = IllegalArgumentException.class)
  public void maxThrowsExceptionEncoded() {
    new Component(2000000000);
  }

  @Test(expected = IllegalArgumentException.class)
  public void maxThrowsExceptionSpecified() {
    new Component((short) 2, (short) 0, (short) 0, (short) 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void maxThrowsExceptionSpecified2() {
    new Component((short) 0, (short) 0, (short) 0, (short) 1000);
  }

  @Test
  public void nextWhenEmpty() {
    assertEquals(
        new Component((short) 0, (short) 0, (short) 0, (short) 1), new Component(0).next());
  }

  @Test
  public void nextEndOfComponent() {
    assertEquals(
        new Component((short) 0, (short) 0, (short) 1, (short) 0), new Component(999).next());
  }

  @Test
  public void nextWhenFull() {
    assertNull(new Component(1999999999).next());
  }

  @Test
  public void pathWorks() {
    final ComponentPath componentPath =
        new Component((short) 0, (short) 1, (short) 2, (short) 3).path(Paths.get("/"));

    final String parent = componentPath.directory.toString().replace("\\", "/");
    final String file = componentPath.file.toString().replace("\\", "/");

    assertEquals("/d0/d1/d2", parent);
    assertEquals("/d0/d1/d2/d3.dpo", file);
  }
}
