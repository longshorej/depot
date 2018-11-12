package depot;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public class Queue implements Closeable {
  private long address;
  private boolean open;

  public Queue(String path) {
    this.address = Native.queueNew(path);
    this.open = true;
  }

  public Optional<Integer> getLastId() {
    requireOpen();

    int lastId = Native.queueLastId(address);

    return lastId < 0 ? Optional.empty() : Optional.of(lastId);
  }

  public void append(byte[] data) {
    requireOpen();

    Native.queueAppend(address, data);
  }

  public boolean isEmpty() {
    requireOpen();

    return Native.queueIsEmpty(address);
  }

  public boolean isFull() {
    requireOpen();

    return Native.queueIsFull(address);
  }

  public Iterator<QueueItem> stream() {
    return stream(-1);
  }

  // @TODO is there an Iterator with Closeable type?
  public QueueIterator stream(long id) {
    requireOpen();

    long streamAddress = Native.queueStream(address, id);

    return new QueueIterator(streamAddress, false);
  }

  public Iterator<QueueItem> streamWithTruncated(long id) {
    requireOpen();

    long streamAddress = Native.queueStream(address, id);

    return new QueueIterator(streamAddress, true);
  }

  public void sync() {
    requireOpen();

    Native.queueSync(address);
  }

  @Override
  public void close() {
    if (open) {
      open = false;
      Native.queueDestroy(address);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    // The user SHOULD call close, but in this
    // case they haven't, so let's cleanup.

    close();

    super.finalize();
  }

  private void requireOpen() {
    if (!open) {
      throw new IllegalStateException("Queue is closed");
    }
  }
}

class QueueIterator implements Iterator<QueueItem>, Closeable {
  private final long address;
  private final boolean allowTruncated;
  private QueueItem next;
  private boolean open;

  public QueueIterator(long address, boolean allowTruncated) {
    this.address = address;
    this.allowTruncated = allowTruncated;
    this.next = null;
    this.open = true;

    advance();
  }

  @Override
  public boolean hasNext() {
    requireOpen();

    return next != null;
  }

  @Override
  public QueueItem next() {
    requireOpen();

    if (next == null) {
      throw new NoSuchElementException();
    }

    QueueItem n = next;

    advance();

    return n;
  }

  @Override
  public void remove() {
    requireOpen();

    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    if (open) {
      open = false;
      Native.queueStreamDestroy(address);
    }
  }

  private void advance() {
    requireOpen();

    long itemPtr = Native.queueStreamNextItem(address);

    while (true) {
      try {
        long itemId = Native.queueStreamItemId(itemPtr);

        if (itemId == -1) {
          next = null;
          return;
        } else {
          int itemLength = Native.queueStreamItemLength(itemPtr);
          boolean truncated = Native.queueStreamItemTruncated(itemPtr);

          if (!truncated || allowTruncated) {
            // @TODO consider a lower level interface to reduce allocations
            byte[] data = new byte[itemLength];
            Native.queueStreamItemCopy(itemPtr, data);

            next = new QueueItem(itemId, data, truncated);
            return;
          }
        }
      } finally {
        Native.queueStreamItemDestroy(itemPtr);
      }
    }
  }

  private void requireOpen() {
    if (!open) {
      throw new IllegalStateException("QueueIterator is closed");
    }
  }

  @Override
  protected void finalize() throws Throwable {
    // The user SHOULD call close, but in this
    // case they haven't, so let's cleanup.

    close();

    super.finalize();
  }
}
