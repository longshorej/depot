package depot.section;

public class Section {
  public static final int MAX_ITEM_SIZE = 8192;
  public static final byte MARKER_ESCAPE = (byte) '\\';
  public static final byte MARKER_SEPARATOR = (byte) '\n';
  public static final byte MARKER_SEPARATOR_REMAP = (byte) '$';
  public static final byte MARKER_FAIL = (byte) '-';
  public static final byte MARKER_FAIL_REMAP = (byte) '.';
}
