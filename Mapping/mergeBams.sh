find $BAM_DIR -name '*.bam' | {
    read firstbam
    samtools view -h "$firstbam"
    while read bam; do
        samtools view "$bam"
    done
} | samtools view -ubS - | samtools sort - merged
samtools index merged.bam
ls -l merged.bam merged.bam.bai