#!/usr/bin/perl
use File::Slurp;
my @spark_versions = ["1.1.1", "1.2.0", "1.3.0"];
# Backup the build file
`cp build.sbt build.sbt_back`;
# Get the original version
my $input = read_file("build.sbt");
my $original_version;
if ($input =~ /version\s+\:\=\s+\"(.*?)\"/) {
    $orginal_version = $1;
} else {
    die "Could not extract version";
}
for my $spark_version (@spark_versions) {
    my $target_version = $spark_version."_".$original_version;
    $new_buld = $input;
    $new_build =~ s/version\s+\:\=\s+\"(.*?)\"/version := "$target_version"/;
    $new_build =~ s/sparkVersion\s+\:\=\s+\"(.*?)\"/sparkVersion := "$spark_version"/;
    open (OUT, ">build.sbt");
    print OUT $new_build;
    close (OUT);
    `./sbt/sbt clean compile package publishSigned spPublish`;
}
`cp build.sbt_back build.sbt`;
`./sbt/sbt clean compile package publishSigned spPublish`;
