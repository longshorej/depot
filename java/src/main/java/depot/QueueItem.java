package depot;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class QueueItem {
  public final long id;
  public final ByteBuffer data;

  public QueueItem(final long id, final ByteBuffer data) {
    this.id = id;
    this.data = data;
  }

  public String toDecodedString() {
    final int position = data.position();
    return new String(data.array(), position, data.limit() - position, StandardCharsets.UTF_8);
  }

  /**
   * Copy this item's data into an `OwnedSectionItem` whose contents will not change (unless
   * modified by the user).
   */
  public OwnedQueueItem toOwned() {
    final int l = data.limit();
    final int p = data.position();
    final byte[] d = new byte[l - p];

    data.get(d, p, d.length);

    return new OwnedQueueItem(id, d);
  }
}
