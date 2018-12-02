package depot.section;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A `SectionItem` is a unit of data that is stored in the section. It contains a reference to its
 * section's buffer, thus its data is only valid until that section is changed by a call to
 * `next()`.
 *
 * <p>If desired, `toOwned` can be called to obtain an object whose data cannot change.
 */
public class SectionItem {
  // @TODO char as byte
  public static final byte TYPE_RAW = 65;
  public static final byte TYPE_ENCODED = 66;
  public static final byte TYPE_REMOVED = 67;

  public final byte type;
  public final int id;
  public final ByteBuffer data;
  public final boolean truncated;

  public SectionItem(
      final byte type, final int id, final ByteBuffer data, final boolean truncated) {
    this.type = type;
    this.id = id;
    this.data = data;
    this.truncated = truncated;
  }

  @Override
  public boolean equals(final Object o) {
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

  /**
   * Copy this item's data into an `OwnedSectionItem` whose contents will not change (unless
   * modified by the user).
   */
  public OwnedSectionItem toOwned() {
    final int l = data.limit();
    final int p = data.position();
    final byte[] d = new byte[l - p];

    data.get(d, p, d.length);

    return new OwnedSectionItem(type, id, d, truncated);
  }

  public String toDecodedString() {
    final int position = data.position();
    return new String(data.array(), position, data.limit() - position, StandardCharsets.UTF_8);
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
