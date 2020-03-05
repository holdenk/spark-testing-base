#!/usr/bin/perl
use File::Slurp;
use strict;
use warnings;
my @spark_versions = (
    "2.0.0", "2.0.1", "2.0.2", "2.1.0",
    "2.1.1", "2.1.2", "2.1.3",
    "2.2.0", "2.2.1", "2.2.2", "2.2.3",
    "2.3.0", "2.3.1", "2.3.2", "2.3.3",
    "2.4.0", "2.4.1", "2.4.2", "2.4.3", "2.4.4", "2.4.5");
# Backup the build file
`cp build.sbt build.sbt_back`;
# Get the original version
my $input = read_file("build.sbt");
foreach my $spark_version (@spark_versions) {
    print "Next spark version ".$spark_version;
    print "\nbuilding\n";
    # Publish local first so kafka sub project can resolve
    print "\nGoing to run: ./sbt/sbt  -DsparkVersion=$spark_version version clean +publishLocal\n";
    print `./sbt/sbt  -DsparkVersion=$spark_version version clean +publishLocal`;
    print "\nGoing to run: ./sbt/sbt  -DsparkVersion=$spark_version version clean +publishSigned\n";
    print `./sbt/sbt  -DsparkVersion=$spark_version version clean +publishSigned`;
    print "\nbuilt\n";
}
