package depot.section;

import java.util.Objects;

/**
 * A `SectionEntry` is a wrapper for a `SectionItem` that also indicates if the end of a section was
 * reached.
 *
 * <p>If `eof` is reached, `item` will be null.
 *
 * <p>Additionally, `item` will be null if it is part of a sequence of items that was removed.
 */
public class SectionEntry {
  public final SectionItem item;
  public final boolean absoluteEof;
  public final boolean eof;
  public final int removed;

  public SectionEntry(SectionItem item, boolean absoluteEof, boolean eof, int removed) {
    this.item = item;
    this.absoluteEof = absoluteEof;
    this.eof = eof;
    this.removed = removed;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SectionEntry that = (SectionEntry) o;
    return absoluteEof == that.absoluteEof
        && eof == that.eof
        && removed == that.removed
        && Objects.equals(item, that.item);
  }

  @Override
  public int hashCode() {

    return Objects.hash(item, absoluteEof, eof, removed);
  }

  @Override
  public String toString() {
    return "SectionEntry{"
        + "item="
        + item
        + ", absoluteEof="
        + absoluteEof
        + ", eof="
        + eof
        + ", removed="
        + removed
        + '}';
  }
}
