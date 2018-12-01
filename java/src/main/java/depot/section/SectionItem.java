package depot.section;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** A `SectionItem` is a unit of data that is stored in the section. */
public class SectionItem {
  // @TODO char as byte
  public static final byte TYPE_RAW = 65;
  public static final byte TYPE_ENCODED = 66;
  public static final byte TYPE_REMOVED = 67;

  public final byte type;
  public final int id;
  public final ByteBuffer data;
  public final boolean truncated;

  public SectionItem(byte type, int id, ByteBuffer data, boolean truncated) {
    this.type = type;
    this.id = id;
    this.data = data;
    this.truncated = truncated;
  }

  public String dataAsString() {
    int position = data.position();
    return new String(data.array(), position, data.limit() - position, StandardCharsets.UTF_8);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SectionItem that = (SectionItem) o;
    return type == that.type
        && id == that.id
        && truncated == that.truncated
        && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id, data, truncated);
  }

  @Override
  public String toString() {
    return "SectionItem{"
        + "type="
        + type
        + ", id="
        + id
        + ", data="
        + data
        + ", truncated="
        + truncated
        + '}';
  }
}
