REGISTER /Users/onson001/Desktop/hadoop/piggybank.jar;
sample_bb_ratio = LOAD '$sample_bb_ratio_input' USING PigStorage('\t') AS (chr:chararray, pos:int, bb_ratio:float);
exon_pileup = LOAD '$exon_pileup_input' USING PigStorage('\t') AS (gene_symbol:chararray, exon_contig_id:chararray, chr:chararray, start:int, end:int, pos:int, contig_length:int);
A = JOIN sample_bb_ratio BY (chr,pos), exon_pileup BY (chr,pos);
B = FOREACH A GENERATE sample_bb_ratio::chr AS chr, sample_bb_ratio::pos AS pos, sample_bb_ratio::bb_ratio AS bb_ratio, exon_pileup::gene_symbol AS gene_symbol;
STORE B INTO '$output' using PigStorage('\t');
