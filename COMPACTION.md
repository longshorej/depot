# Depot Compaction Thoughts

* Allow a section to be rewritten to exclude one or more ids.

* Introduce a marker byte to each section (first byte of file)

* Regular section and compacted section, with slightly different impls

* If compacted, the major problem is mapping offsets.
* However, compaction happens on older files.
* Typically, consumers will mostly be up to date
* Therefore, a performance penalty in resuming from an old offset is acceptable.

* Store a mapping for each item in the file, from old to new, at the beginning of the file

* thus, 8 bytes per item, constant overhead, even if deleted.

* Section iterating to be expanded -- cases are Item, TruncatedItem, RemovedItem

* an empty section may be completely dropped -- when resuming, return with "RemovedItem"
  and end of section

* resuming from offset is a linear search
