package depot.section;

/**
 * A `SectionEntry` is a wrapper for a `SectionEntry` and also indicates if the end of a section was
 * reached.
 *
 * <p>`item` may be null if the last item in this section was removed.
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
}
