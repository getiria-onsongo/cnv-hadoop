REGISTER /Users/onson001/Desktop/hadoop/piggybank.jar;

-- SEPARATE DATA IN coverage_ratio INTO FIELDS WE CAN JOIN WITH bb_ratio_gene
A = LOAD '$coverage_ratio_input' USING PigStorage('\t') AS (ref_chr_pos:chararray, coverage_ratio:float);
B = FOREACH A GENERATE REPLACE(ref_chr_pos,';',','),coverage_ratio;
STORE B INTO '$temp1' using PigStorage(',');
C = LOAD '$temp1' USING PigStorage(',') AS (ref_contig:chararray, chr_pos:chararray, coverage_ratio:float);
D = FOREACH C GENERATE ref_contig, REPLACE(chr_pos,':',','),coverage_ratio;
STORE D INTO '$temp2' using PigStorage(',');
E = LOAD '$temp2' USING PigStorage(',') AS (ref_contig:chararray, chr:chararray, pos:int, coverage_ratio:float);

-- LOAD bb_ratio_gene SO WE CAN JOIN
F = LOAD '$bb_ratio_gene_input' USING PigStorage('\t') AS (chr:chararray,pos:int,bb_ratio:float,gene_symbol:chararray);

-- JOIN E and F
G = JOIN E BY (chr,pos), F BY (chr,pos);
H = FOREACH G GENERATE E::ref_contig AS ref_contig,F::gene_symbol AS gene_symbol,F::chr AS chr,F::pos AS pos,E::coverage_ratio AS coverage_ratio,F::bb_ratio AS bb_ratio;

-- SEPARATE THE THREE REFERENCES
J = FOREACH H GENERATE ref_contig;
K = DISTINCT J;
L = RANK K;
ref1_id = FILTER L BY rank_K == 1;
ref2_id = FILTER L BY rank_K == 2;
ref3_id = FILTER L BY rank_K == 3;
ref1_j = JOIN ref1_id BY (ref_contig), H BY (ref_contig);
ref2_j = JOIN ref2_id BY (ref_contig), H BY (ref_contig);
ref3_j = JOIN ref3_id BY (ref_contig), H BY (ref_contig);
ref1 = FOREACH ref1_j GENERATE H::ref_contig AS ref_contig,H::gene_symbol AS gene_symbol,H::chr AS chr,H::pos AS pos,H::coverage_ratio AS coverage_ratio,H::bb_ratio AS bb_ratio;
ref2 = FOREACH ref2_j GENERATE H::ref_contig AS ref_contig,H::gene_symbol AS gene_symbol,H::chr AS chr,H::pos AS pos,H::coverage_ratio AS coverage_ratio,H::bb_ratio AS bb_ratio;
ref3 = FOREACH ref3_j GENERATE H::ref_contig AS ref_contig,H::gene_symbol AS gene_symbol,H::chr AS chr,H::pos AS pos,H::coverage_ratio AS coverage_ratio,H::bb_ratio AS bb_ratio;
STORE ref1 INTO '$output1' using PigStorage('\t');
STORE ref2 INTO '$output2' using PigStorage('\t');
STORE ref3 INTO '$output3' using PigStorage('\t');

