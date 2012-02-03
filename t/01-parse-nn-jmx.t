use strict;
use warnings;
use Test::More;

my %attributes=(
    _test_namenodeinfo=> 't/data/ab.namenodeinfo',
    _test_jobtrackerinfo=> 't/data/ab.jobtrackerinfo',
    );

use Hadoop::Admin;
my $ha=new Hadoop::Admin(%attributes);

my @dn_live_list=$ha->datanode_live_list();
is($#dn_live_list, 450, "Parse Live List");
done_testing();
