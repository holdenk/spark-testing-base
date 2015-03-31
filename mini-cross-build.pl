#!/usr/bin/perl
use File::Slurp;
use strict;
use warnings;
my @spark_versions = ("1.1.1", "1.2.0", "1.3.0");
# Backup the build file
`cp build.sbt build.sbt_back`;
# Get the original version
my $input = read_file("build.sbt");
my $original_version;
if ($input =~ /version\s+\:\=\s+\"(.+?)\"/) {
    $original_version = $1;
} else {
    die "Could not extract version";
}
print "Cross building for $original_version";
foreach my $spark_version (@spark_versions) {
    my $target_version = $spark_version."_".$original_version;
    print "New target version ".$target_version;
    my $new_build = $input;
    $new_build =~ s/version\s+\:\=\s+\".+?\"/version := "$target_version"/;
    $new_build =~ s/sparkVersion\s+\:\=\s+\".+?\"/sparkVersion := "$spark_version"/;
    print "new build file $new_build hit";
    open (OUT, ">build.sbt");
    print OUT $new_build;
    close (OUT);
    print "building";
    print `./sbt/sbt clean compile package publishSigned spPublish`;
    print "built"
}
`cp build.sbt_back build.sbt`;
`./sbt/sbt clean compile package publishSigned spPublish`;
