#!/usr/bin/env bash

read offset fileLine

SampleName=$(echo "$fileLine" | cut -f1)
forwardReads=$(echo "$fileLine" | cut -f2)
reverseReads=$(echo "$fileLine" | cut -f3)

echo "SampleName: $SampleName, forward reads: $forwardReads, reverser reads: $reverseReads"
 
