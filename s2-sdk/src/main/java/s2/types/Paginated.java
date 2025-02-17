package s2.types;

import java.util.List;

public class Paginated<T> {
  public final boolean hasMore;
  public final List<T> elems;

  public Paginated(boolean hasMore, List<T> elems) {
    this.hasMore = hasMore;
    this.elems = elems;
  }
}
