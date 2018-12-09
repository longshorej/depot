package depot;

import java.util.Arrays;
import java.util.Objects;

public class OwnedQueueItem {
  public final long id;
  public final byte[] data;

  public OwnedQueueItem(final long id, final byte[] data) {
    this.id = id;
    this.data = data;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final OwnedQueueItem that = (OwnedQueueItem) o;
    return id == that.id && Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(id);
    result = 31 * result + Arrays.hashCode(data);
    return result;
  }

  @Override
  public String toString() {
    return "OwnedQueueItem{" + "id=" + id + ", data=" + Arrays.toString(data) + '}';
  }
}
