package s2.types;

import java.util.List;

public record Paginated<T>(boolean hasMore, List<T> elems) {}
