REGISTER /Users/onson001/Desktop/hadoop/piggybank.jar;
cov_data = LOAD '$cov_input' USING PigStorage('\t') AS (chr:chararray, chr_pos:int, coverage:int);
ref_data = LOAD '$ref_input' USING PigStorage('\t') AS (exon_contig_id:chararray, gene:chararray, chr:chararray, chr_pos:int);
A = JOIN ref_data BY (chr,chr_pos), cov_data BY (chr,chr_pos);
B = FOREACH A GENERATE ref_data::exon_contig_id AS exon_contig_id, ref_data::chr AS chr, ref_data::chr_pos AS pos, cov_data::coverage AS coverage;
STORE B INTO '$output' using PigStorage('\t');
