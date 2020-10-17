package depot.section;

/**
 * A `Section` is the primitive storage unit in Depot. A section can contain roughly 2GB of data,
 * though in some cases this may be a few KB more.
 *
 * <p>To write items to a section, reference `SectionWriter`.
 *
 * <p>To read items from a section, reference `SectionStreamer`.
 *
 * <p>To remove items from a section (via rewrite), reference `SectionCompactor`.
 */
public class Section {
  public static final int MAX_ITEM_SIZE = 8192;
  public static final byte MARKER_ESCAPE = (byte) '\\';
  public static final byte MARKER_SEPARATOR = (byte) '\n';
  public static final byte MARKER_SEPARATOR_REMAP = (byte) '$';
  public static final byte MARKER_FAIL = (byte) '-';
  public static final byte MARKER_FAIL_REMAP = (byte) '.';
}
