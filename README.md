# SYNOPSIS

    use Hadoop::Admin; 

    my $cluster=Hadoop::Admin->new({
      'namenode'          => 'namenode.host.name',
      'jobtracker'        => 'jobtracker.host.name',
    });

    print $cluster->datanode_live_list();

# DESCRIPTION

This module connects to Hadoop servers using http.  The JMX Proxy
Servlet is queried for specific mbeans.

This module requires Hadoop the changes in
https://issues.apache.org/jira/browse/HADOOP-7144.  They are available
in versions 0.20.204.0, 0.23.0 or later.

# INTERFACE FUNCTIONS

## new ()

- Description

    Create a new instance of the Hadoop::Admin class.  

    The method requires a hash containing at minimum one of the
    namenode's, the resourcemanager's, and the jobtracker's hostnames.
    Optionally, you may provide a socksproxy for the http connection.  Use
    of both a jobtracker and resourcemanger is prohibited.  It is not a
    valid cluster configuration to have both a jobtracker and a
    resourcemanager.

    Creation of this object will cause an immediate querry to servers
    provided to the constructor.

- namenode => <hostname>
- namenode\_port => <port number>
- jobtracker => <hostname>
- jobtracker\_port => <port number>
- resourcemanager => <hostname>
- resourcemanager\_port => <port number>
- socksproxy => <hostname>
- socksproxy\_port => <port number>

## datanode\_live\_list ()

- Description

    Returns a list of the current live DataNodes.

- Return values

    Array containing hostnames.

## datanode\_dead\_list ()

- Description

    Returns a list of the current dead DataNodes.

- Return values

    Array containing hostnames.

## datanode\_decom\_list ()

- Description

    Returns a list of the currently decommissioning DataNodes.

- Return values

    Array containing hostnames.

## nodemanager\_live\_list ()

- Description

    Returns a list of the current live NodeManagers.

- Return values

    Array containing hostnames.

## tasktracker\_live\_list ()

- Description

    Returns a list of the current live TaskTrackers.

- Return values

    Array containing hostnames.

## tasktracker\_blacklist\_list ()

- Description

    Returns a list of the current blacklisted TaskTrackers.

- Return values

    Array containing hostnames.

## tasktracker\_graylist\_list ()

- Description

    Returns a list of the current graylisted TaskTrackers.

- Return values

    Array containing hostnames.

# KNOWN BUGS

None known at this time.  Please log issues at: 

https://github.com/cwimmer/hadoop-admin/issues

# AVAILABILITY

Source code is available on GitHub:

https://github.com/cwimmer/hadoop-admin

Module available on CPAN as Hadoop::Admin:

http://search.cpan.org/~cwimmer/
