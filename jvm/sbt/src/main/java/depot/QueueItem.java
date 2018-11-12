package depot;

public class QueueItem {
  public final long id;
  public final byte[] data;
  public final boolean truncated;

  public QueueItem(long id, byte[] data, boolean truncated) {
    this.id = id;
    this.data = data;
    this.truncated = truncated;
  }

  @Override
  public String toString() {
    return "QueueItem(" + id + ", " + data + ", " + truncated + ")";
  }
}
