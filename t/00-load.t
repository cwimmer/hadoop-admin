use strict;
use warnings;
use Test::More;

BEGIN { use_ok('Hadoop::Admin') };

can_ok('Hadoop::Admin', ('new'));

my %attributes=(
    namenode          => 'a',
    jobtracker        => 'b',
    secondarynamenode => 'c',
    socksproxy        => 'd', 
    _test_namenodeinfo=> 't/data/ab.namenodeinfo',
    _test_jobtrackerinfo=> 't/data/ab.jobtrackerinfo',
    );

use Hadoop::Admin;
my $ha=new Hadoop::Admin(%attributes);

isa_ok($ha, 'Hadoop::Admin');
is($ha->get_namenode(), 'a', "get_namenode() works");
is($ha->get_jobtracker(), 'b', "get_jobtracker() works");
is($ha->get_secondarynamenode, 'c', "get_secondarynamenode() works");
is($ha->get_socksproxy(), 'd', "get_socksproxy() works");
done_testing();
