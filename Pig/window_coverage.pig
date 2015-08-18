REGISTER /Users/onson001/Desktop/hadoop/piggybank.jar;
cov_data = LOAD '$cov_data_input' USING PigStorage('\t') AS (chr:chararray, chr_pos:int, coverage:int);
window_data = LOAD '$window_data_input' USING PigStorage('\t') AS (window_id:chararray, chr:chararray, chr_pos:int);
A = JOIN window_data BY (chr,chr_pos) LEFT OUTER, cov_data BY (chr,chr_pos);
B = FOREACH A GENERATE window_data::window_id AS window_id, window_data::chr AS chr, window_data::chr_pos AS pos, cov_data::coverage AS coverage;
STORE B INTO '$window_coverage_output' using PigStorage('\t');
