#!/usr/bin/perl
use File::Slurp;
use strict;
use warnings;
my @spark_versions = (
    "2.4.0", "2.4.1", "2.4.2", "2.4.3", "2.4.4", "2.4.5", "2.4.6", "2.4.7",
    "3.0.0", "3.0.1", "3.0.2",
    "3.1.1", "3.1.2"
    );
# Backup the build file
`cp build.sbt build.sbt_back`;
# Get the original version
my $input = read_file("build.sbt");
foreach my $spark_version (@spark_versions) {
    print "Next spark version ".$spark_version;
    print "\nbuilding\n";
    # Publish local first so kafka sub project can resolve
    print "\nGoing to run: ./build/sbt  -DsparkVersion=$spark_version version clean +publishLocal\n";
    print `./build/sbt  -DsparkVersion=$spark_version version clean +publishLocal`;
    print "\nGoing to run: ./build/sbt  -DsparkVersion=$spark_version version clean +publishSigned\n";
    print `./build/sbt  -DsparkVersion=$spark_version version clean +publishSigned`;
    print "\nbuilt\n";
}
