package depot.section;

import java.util.Arrays;
import java.util.Objects;

public class OwnedSectionItem {
  public final byte type;
  public final int id;
  public final byte[] data;
  public final boolean truncated;

  public OwnedSectionItem(
      final byte type, final int id, final byte[] data, final boolean truncated) {
    this.type = type;
    this.id = id;
    this.data = data;
    this.truncated = truncated;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final OwnedSectionItem that = (OwnedSectionItem) o;
    return type == that.type
        && id == that.id
        && truncated == that.truncated
        && Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(type, id, truncated);
    result = 31 * result + Arrays.hashCode(data);
    return result;
  }

  @Override
  public String toString() {
    return "OwnedSectionItem{"
        + "type="
        + type
        + ", id="
        + id
        + ", data="
        + Arrays.toString(data)
        + ", truncated="
        + truncated
        + '}';
  }
}
