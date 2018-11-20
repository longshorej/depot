package depot.section;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class SectionItem {
  public final int id;
  public final ByteBuffer data;
  public final boolean knownEof;
  public final boolean truncated;

  public SectionItem(int id, ByteBuffer data, boolean knownEof, boolean truncated) {
    this.id = id;
    this.data = data;
    this.knownEof = knownEof;
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
    return id == that.id
        && knownEof == that.knownEof
        && truncated == that.truncated
        && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {

    return Objects.hash(id, data, knownEof, truncated);
  }
}
