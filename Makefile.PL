use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME => 'Hadoop::Admin',
    ABSTRACT_FROM => 'lib/Hadoop/Admin.pm',
    AUTHOR => [ 'Charles Wimmer', 'charles@wimmer.net' ],
    VERSION_FROM => 'lib/Hadoop/Admin.pm',
    PREREQ_PM         => {
	"LWP::UserAgent"       => 0,
	"JSON"                 => 0,
	"XML::LibXML"          => 0,
	"TAP::Harness::JUnit"  => 0,
	"LWP::Protocol::https" => 0,
	"LWP::Protocol::socks" => 0,
    },
    );
