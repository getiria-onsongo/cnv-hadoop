REGISTER /Users/onson001/Desktop/hadoop/piggybank.jar;
sample_data = LOAD '$sample_input' USING PigStorage('\t') AS (key:chararray, within_ratio:float);
control_data = LOAD '$control_input' USING PigStorage('\t') AS (key:chararray, within_ratio:float);
A = JOIN sample_data BY (key), control_data BY (key);
B = FOREACH A GENERATE sample_data::key AS key, sample_data::within_ratio AS s_within_ratio, control_data::within_ratio AS c_within_ratio;
C = FOREACH B GENERATE key, s_within_ratio/c_within_ratio AS coverage_ratio;
STORE C INTO '$output' using PigStorage('\t');
