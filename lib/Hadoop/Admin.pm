# ****************************************************************************
#
#   POD HEADER
#
# ****************************************************************************

=head1 NAME

Hadoop::Admin - Module for administration of Hadoop clusters

=head1 SYNOPSIS

    use Hadoop::Admin; 

    my $cluster=Hadoop::Admin->new({
      'namenode'          => 'namenode.host.name',
      'jobtracker'        => 'jobtracker.host.name',
    });

    print $cluster->datanode_live_list();


=head1 DESCRIPTION

This module connects to Hadoop servers using http.  The JMX Proxy
Servlet is queried for specific mbeans.

This module requires Hadoop the changes in
https://issues.apache.org/jira/browse/HADOOP-7144.  They are available
in versions 0.20.204.0, 0.23.0 or later.

=head1 INTERFACE FUNCTIONS

=for comment After this the Puclic interface functions are introduced
=for comment you close the blockquote by inserting POD footer

=for html
<BLOCKQUOTE>

=cut


package Hadoop::Admin;


use strict;
use warnings;
use LWP::UserAgent;
use JSON -support_by_pp;

use version;
our $VERSION='0.2_02';
$VERSION = eval $VERSION;


=pod

=head2 new ()

=over 4

=item Description

Create a new instance of the Hadoop::Admin class.  

The method requires a hash containing at minimum the namenode's, and
the jobtracker's hostnames.  Optionally, you may provide a socksproxy
for the http connection.

Creation of this object will cause an immediate querry to both the
NameNode and JobTracker.

=item namenode => <hostname>

=item jobtracker => <hostname>

=item socksproxy => <hostname>

=item Returns newly created object.

=back

=cut
sub new(){

    Carp::croak("Options should be key/value pairs, not hash reference") 
        if ref($_[1]) eq 'HASH'; 

    my($class, %cnf) = @_;

    my $self={
	conf=>{%cnf},
    };

    if ( exists $self->{'conf'}->{'namenode'} ){
	$self->{'namenode'}=$self->{'conf'}->{'namenode'};
    }
    if ( exists $self->{'conf'}->{'jobtracker'} ){
	$self->{'jobtracker'}=$self->{'conf'}->{'jobtracker'};
    }
    if ( exists $self->{'conf'}->{'secondarynamenode'} ){
	$self->{'secondarynamenode'}=$self->{'conf'}->{'secondarynamenode'};
    }
    if ( exists $self->{'conf'}->{'socksproxy'} ){
	$self->{'socksproxy'}=$self->{'conf'}->{'socksproxy'};
    }

    $self->{'ua'} = new LWP::UserAgent();
    if ( exists $self->{'socksproxy'} ){
	$self->{'ua'}->proxy([qw(http https)] => 'socks://'.$self->{'socksproxy'}.':1080');
    }

    bless($self,$class);

    ## Hooks for testing during builds.  Doesn't connect to a real cluster.
    if ( exists $self->{'conf'}->{'_test_namenodeinfo'} ){
	my $test_nn_string;
	{
	    local $/=undef;
	    open my $fh, $self->{'conf'}->{'_test_namenodeinfo'} or die "Couldn't open file: $!";
	    $test_nn_string = <$fh>;
	    close $fh;
	}
	$self->parse_nn_jmx($test_nn_string);
    }else{
	if ( exists $self->{'namenode'} ){
	    $self->gather_nn_jmx('NameNodeInfo');
	}
    }
    
    if ( exists $self->{'conf'}->{'_test_jobtrackerinfo'} ){
	my $test_jt_string;
	{
	    local $/=undef;
	    open my $fh, $self->{'conf'}->{'_test_jobtrackerinfo'} or die "Couldn't open file: $!";
	    $test_jt_string = <$fh>;
	    close $fh;
	}
	$self->parse_jt_jmx($test_jt_string);
    }else{
	if ( exists $self->{'jobtracker'} ){
	    $self->gather_jt_jmx('JobTrackerInfo');
	}
    }
    
    return $self;
}


=pod

=head2 get_namenode ()

=over 4

=item Description

Returns the JobTracker from instantiation

=back

=cut
sub get_namenode(){
    my $self=shift;
    return $self->{'namenode'};
}

=pod

=head2 get_namenode ()

=over 4

=item Description

Returns the JobTracker from instantiation

=back

=cut
sub get_jobtracker(){
    my $self=shift;
    return $self->{'jobtracker'};
}

sub get_secondarynamenode(){
    my $self=shift;
    return $self->{'secondarynamenode'};
}

=pod

=head2 get_namenode ()

=over 4

=item Description

Returns the Socks Proxy from instantiation

=back

=cut
sub get_socksproxy(){
    my $self=shift;
    return $self->{'socksproxy'};
}


=pod

=head2 datanode_live_list ()

=over 4

=item Description

Returns a list of the current live DataNodes.

=item Return values

Array containing hostnames.

=back

=cut
sub datanode_live_list(){
    my $self=shift;
    return keys %{$self->{'NameNodeInfo_LiveNodes'}};
}

=pod

=head2 datanode_dead_list ()

=over 4

=item Description

Returns a list of the current dead DataNodes.

=item Return values

Array containing hostnames.

=back

=cut
sub datanode_dead_list(){
    my $self=shift;
    return keys %{$self->{'NameNodeInfo_DeadNodes'}};
}

=pod

=head2 datanode_decom_list ()

=over 4

=item Description

Returns a list of the currently decommissioning DataNodes.

=item Return values

Array containing hostnames.

=back

=cut
sub datanode_decom_list(){
    my $self=shift;
    return keys %{$self->{'NameNodeInfo_DecomNodes'}};
}


=pod

=head2 tasktracker_live_list ()

=over 4

=item Description

Returns a list of the current live TaskTrackers.

=item Return values

Array containing hostnames.

=back

=cut
sub tasktracker_live_list(){
    my $self=shift;
    my @returnValue=();
    use Data::Dumper;
    foreach my $hostref ( @{$self->{'JobTrackerInfo_AliveNodesInfoJson'}} ) {
	push @returnValue, $hostref->{'hostname'};
    }
    return @returnValue;
}

=pod

=head2 tasktracker_live_list ()

=over 4

=item Description

Returns a list of the current blacklisted TaskTrackers.

=item Return values

Array containing hostnames.

=back

=cut
sub tasktracker_blacklist_list(){
    my $self=shift;
    my @returnValue=();
    foreach my $hostref ( @{$self->{'JobTrackerInfo_BlacklistedNodesInfoJson'}} ) {
	push @returnValue, $hostref->{'hostname'};
    }
    return @returnValue;
}

=pod

=head2 tasktracker_live_list ()

=over 4

=item Description

Returns a list of the current graylisted TaskTrackers.

=item Return values

Array containing hostnames.

=back

=cut
sub tasktracker_graylist_list(){
    my $self=shift;
    my @returnValue=();
    foreach my $hostref ( @{$self->{'JobTrackerInfo_BlacklistedNodesInfoJson'}} ) {
	push @returnValue, $hostref->{'hostname'};
    }
    return @returnValue;
}

sub gather_nn_jmx($$){
    my $self=shift;
    my $bean=shift;
    my $qry;
    if ($bean eq 'NameNodeInfo'){
	$qry='Hadoop%3Aservice%3DNameNode%2Cname%3DNameNodeInfo';
    }
    my $jmx_url= "http://".$self->{'namenode'}.":50070/jmx?qry=$qry";
    my $response = $self->{'ua'}->get($jmx_url);
    if (! $response->is_success) {
	print "Can't get JMX data from Namenode: $@";
	exit(1);
    }
    $self->parse_nn_jmx($response->decoded_content);
}

sub parse_nn_jmx($$){
    my $self=shift;
    my $nn_content=shift;
    my $json=new JSON();
    my $json_text = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->pretty->decode($nn_content);
    foreach my $bean (@{$json_text->{beans}}){
	if ($bean->{name} eq "Hadoop:service=NameNode,name=NameNodeInfo"){
	    foreach my $var (keys %{$bean}){
		$self->{"NameNodeInfo_$var"}=$bean->{$var};
	    }
	    $self->{'NameNodeInfo_LiveNodes'}=$json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->pretty->decode($bean->{LiveNodes});
	    $self->{'NameNodeInfo_DeadNodes'}=$json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->pretty->decode($bean->{DeadNodes});
	    $self->{'NameNodeInfo_DecomNodes'}=$json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->pretty->decode($bean->{DecomNodes});
	}
	
    }
}

sub gather_jt_jmx($$){
    my $self=shift;
    my $bean=shift;
    my $qry;
    if ($bean eq "JobTrackerInfo"){
	$qry='Hadoop%3Aservice%3DJobTracker%2Cname%3DJobTrackerInfo';
    }
    my $jmx_url= "http://".$self->{'jobtracker'}.":50030/jmx?qry=$qry";
    my $response = $self->{'ua'}->get($jmx_url);
    if (! $response->is_success) {
	print "Can't get JMX data from Namenode: $@";
	exit(1);
    }
    $self->parse_jt_jmx($response->decoded_content);

}

sub parse_jt_jmx(){
    my $self=shift;
    my $jt_content=shift;
    my $json=JSON->new();
    my $json_text = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->pretty->decode($jt_content);
    foreach my $bean (@{$json_text->{beans}}){
	foreach my $var (keys %{$bean}){
	    $self->{"JobTrackerInfo_$var"}=$bean->{$var};
	}
	if ($bean->{name} eq "Hadoop:service=JobTracker,name=JobTrackerInfo"){
	    $self->{'JobTrackerInfo_AliveNodesInfoJson'}=$json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->pretty->decode($bean->{AliveNodesInfoJson});
	    $self->{'JobTrackerInfo_BlacklistedNodesInfoJson'}=$json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->pretty->decode($bean->{BlacklistedNodesInfoJson});
	    $self->{'JobTrackerInfo_GraylistedNodesInfoJson'}=$json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->pretty->decode($bean->{GraylistedNodesInfoJson});
	}
	
    }
}

1;
# ****************************************************************************
#
#   POD FOOTER
#
# ****************************************************************************

=pod

=for html
</BLOCKQUOTE>

=head1 KNOWN BUGS

None known at this time.  Please log issues at: 

https://github.com/cwimmer/hadoop-admin/issues

=head1 AVAILABILITY

Source code is available on GitHub:

https://github.com/cwimmer/hadoop-admin

Module available on CPAN as Hadoop::Admin:

http://search.cpan.org/~cwimmer/

=head1 AUTHOR

Copyright (C) 2012 Charles Wimmer.
This program is free software; you can redistribute and/or modify program
under the same terms as Perl itself or in terms of Gnu General Public
license v2 or later.

=cut
