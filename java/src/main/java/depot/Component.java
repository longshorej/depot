package depot;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;

class Component {
  private static final int MaxEncodedValue = 2000000000;
  private static final short MaxValue = 1000;

  private final short one;
  private final short two;
  private final short three;
  private final short four;

  static Map.Entry<Component, Integer> decodeId(final long id) {
    final int componentId = (int) (id >> 32);
    final int sectionId = (int) (id);

    return new AbstractMap.SimpleEntry<>(new Component(componentId), sectionId);
  }

  Component() {
    one = 0;
    two = 0;
    three = 0;
    four = 0;
  }

  Component(final int encoded) {
    if (encoded >= MaxEncodedValue) {
      throw new IllegalArgumentException("encoded component exceeds maximum value");
    }

    one = (short) (encoded / (MaxValue * MaxValue * MaxValue));
    two = (short) ((encoded % (MaxValue * MaxValue * MaxValue)) / (MaxValue * MaxValue));
    three = (short) ((encoded % (MaxValue * MaxValue)) / MaxValue);
    four = (short) (encoded % MaxValue);
  }

  Component(final short one, final short two, final short three, final short four) {
    if (one < 0
        || one > 1
        || two < 0
        || two >= MaxValue
        || three < 0
        || three >= MaxValue
        || four < 0
        || four >= MaxValue) {
      throw new IllegalArgumentException("invalid component value");
    }

    this.one = one;
    this.two = two;
    this.three = three;
    this.four = four;
  }

  int encode() {
    return one * MaxValue * MaxValue * MaxValue
        + two * MaxValue * MaxValue
        + three * MaxValue
        + four;
  }

  long encodeId(final int sectionId) {
    final int componentId = encode();

    return ((long) componentId << 32) | sectionId;
  }

  boolean isEmpty() {
    return four == 0 && three == 0 && two == 0 && one == 0;
  }

  boolean isFull() {
    short m = MaxValue - 1;
    return one == 1 && two == m && three == m && four == m;
  }

  Component next() {
    if (four < MaxValue - 1) {
      return new Component(one, two, three, (short) (four + 1));
    } else if (three < MaxValue - 1) {
      return new Component(one, two, (short) (three + 1), (short) 0);
    } else if (two < MaxValue - 1) {
      return new Component(one, (short) (two + 1), (short) 0, (short) 0);
    } else if (one < 1) {
      return new Component((short) (one + 1), (short) 0, (short) 0, (short) 0);
    } else {
      return null;
    }
  }

  ComponentPath path(final Path base) {
    Path directory = base.resolve("d" + one).resolve("d" + two).resolve("d" + three);
    Path file = directory.resolve("d" + four + ".dpo");

    return new ComponentPath(directory, file);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final Component component = (Component) o;
    return one == component.one
        && two == component.two
        && three == component.three
        && four == component.four;
  }

  @Override
  public int hashCode() {
    return Objects.hash(MaxEncodedValue, MaxValue, one, two, three, four);
  }

  @Override
  public String toString() {
    return "Component{"
        + ", one="
        + one
        + ", two="
        + two
        + ", three="
        + three
        + ", four="
        + four
        + '}';
  }
}
