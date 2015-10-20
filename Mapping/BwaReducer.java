/**
* @param key is a <chrID><;><analysis_id>
*   where chrID is in (1, 2, 3, ..., 23)
* @param value is ignored (not used)
**/
reduce(key, value) {
    DNASeq.mergeAllChromosomesAndPartition(key);
}
