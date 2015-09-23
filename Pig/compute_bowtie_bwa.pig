REGISTER /Users/onson001/Desktop/hadoop/piggybank.jar;
sample_bowtie = LOAD '$sample_bowtie_input' USING PigStorage('\t') AS (chr:chararray, pos:int, coverage:float);
sample_bwa = LOAD '$sample_bwa_input' USING PigStorage('\t') AS (chr:chararray, pos:int, coverage:float);
A = JOIN sample_bowtie BY (chr,pos), sample_bwa BY (chr,pos);
B = FOREACH A GENERATE sample_bwa::chr AS chr, sample_bwa::pos AS pos, sample_bwa::coverage AS bwa_coverage, sample_bowtie::coverage AS bowtie_coverage;
C = FOREACH B GENERATE chr,pos, bowtie_coverage/bwa_coverage AS bowtie_bwa_ratio;
STORE C INTO '$output' using PigStorage('\t');
