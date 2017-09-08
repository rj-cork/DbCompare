package Logger;

# Logger package - functions for printing debug messages and results presentation
# Version 0.01
# (C) 2016 - Radoslaw Karas <rj.cork@gmail.com>
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

use strict;
use warnings;
#use threads;
#use threads::shared;
#use Thread::Semaphore;
#use Thread::Queue;
use POSIX;
use Time::HiRes;
use Data::Dumper;
use FileHandle;
use File::Path qw(make_path);
use Carp;# qw(cluck longmess shortmess);

use constant ERROR => -2;
use constant WARNING => -1;
use constant INFO => 0;
use constant DEBUG => 1;
use constant DEBUG1 => 1;
use constant DEBUG2 => 2;
use constant DEBUG3 => 3;
use constant DEBUGNOFILE => 128;
use constant DEBUGNOSTDOUT => 256;
use constant DEBUGTRACE => 512;#Carp::longmess

$|=1;
my $LOG_FILE;# :shared;
my $OUTPUT; #pipe or STDOUT
my $RESULTS_DIR;
my $DEBUG_LEVEL = 0;
my $RESULT_FILE_FD;
my $RESULT_SEQUENCE = undef; #set to 1 to enable - shows sequence for each output line
			# for checking correct msg flow from coordinator to main process

#############################################################################


# SetupLogger ( { LOG_FILE=>'log.txt', RESULTS_DIR => 'dump_dir', DEBUG_LEVEL=>2} );
sub SetupLogger {
	my $params = shift;

	
	$LOG_FILE = $params->{'LOG_FILE'} if (defined($params->{'LOG_FILE'}));
	$RESULTS_DIR = $params->{'RESULTS_DIR'} if (defined($params->{'RESULTS_DIR'}));
	$DEBUG_LEVEL = $params->{'DEBUG_LEVEL'} if (defined($params->{'DEBUG_LEVEL'}));
	$RESULT_FILE_FD = $params->{'RESULT_FILE_FD'} if (defined($params->{'RESULT_FILE_FD'}));
	$RESULT_SEQUENCE = 1 if (defined($params->{'RESULT_SEQUENCE'}));

	if ( defined($LOG_FILE) && -e $LOG_FILE && ( ! -f $LOG_FILE || -w $LOG_FILE ) ) {
		Terminate("File $LOG_FILE is not a regular or writeable file.");
	}

}

sub SetDebugLevel {
	$DEBUG_LEVEL = shift;
}

sub Terminate {
	my $msg = shift;
	
	$msg = 'Terminating.' if (!defined($msg));

	PrintMsg(ERROR, Carp::longmess($msg));
	exit 1;
}


# PrintMsg (
#	lvl || flags, #message visible on given level or higher, it can be summed logicaly with debug flags
#	text_tag,
#	message
#	);
sub PrintMsg {
	my ($lvl, $tag, $msg) = (0, '', '');
	my $no_stdout = 0;
	my $no_file = 0;
	my $tracle_on = 0;

	($lvl, $tag, $msg) = (shift,shift,shift) if (scalar(@_) == 3);
	($lvl, $msg) = (shift,shift) if (scalar(@_) == 2);
	$msg = shift if (scalar(@_) == 1);


	if ($lvl & DEBUGNOFILE) { #if set dont log it into the logfile
		$no_file = 100;
		$lvl &= ~DEBUGNOFILE;
	}

	if ($lvl & DEBUGNOSTDOUT) { #if set log it into the logfile only
		$no_stdout = 100;
		$lvl &= ~DEBUGNOSTDOUT;
	}

	return if ($lvl > $DEBUG_LEVEL); #skip if current debug level is lower than lvl of the message

	$tag = '[ERROR] '.$tag if ($lvl == ERROR);
	$tag = '[WARNING] '.$tag if ($lvl == WARNING);

	if ($lvl < 0) { #errors and warnings go to STDERR
		print STDERR $tag, $msg;
	} else {	#others to STDOUT if allowed to
		print STDOUT $tag, $msg if ($no_stdout == 0);
	}

	if (defined($LOG_FILE) && $no_file == 0) {
		if ( ! -e $LOG_FILE || ( -f $LOG_FILE && -w $LOG_FILE ) ) {

			lock($LOG_FILE);
			#TODO: flock($LOG_FILE);
			my $f;

			open $f,">>$LOG_FILE";
			print $f $msg, @_, "\n";
			close $f;

	        } else {
			PrintMsg(ERROR || DEBUGNOFILE, Carp::longmess("File $LOG_FILE is not a regular or writeable file. Exiting."));
			exit 1;
	        }
	}

	if (defined($RESULT_FILE_FD)) { #TODO: pomysl jest zeby byl jakis counter z numerem lini na poczatku kazdej, zeby proces odbierajacy z pipea wiedzial
					# ze wszystko jest ok
		print $RESULT_FILE_FD, "<$RESULT_SEQUENCE>", @_, "\n";
		$RESULT_FILE_FD->flush();
		$RESULT_SEQUENCE++;
	} else {
		print STDERR @_, "\n";
		STDERR->flush(); #force flushing - it may be end of a pipe
	}

}


sub OpenResultFile {
	my ($owner, $table, $partition) = @_;
	my $err;

	my $file_name = "$RESULTS_DIR/";

	(defined($partition)) ? ($file_name .= "$owner.$table.$partition.txt") : ($file_name .= "$owner.$table.txt");

	if ( ! -e $RESULTS_DIR ) { #directory doesn't exists
		#try to create
		File::Path::make_path($RESULTS_DIR, {error => \$err}) or Terminate("Cannot create results directory $RESULTS_DIR\n");	

	} elsif (! -d $RESULTS_DIR || ! -w $RESULTS_DIR || ! -x $RESULTS_DIR) {

		Terminate "Cannot create files in directory $RESULTS_DIR\n";

	}

	open $RESULT_FILE_FD, ">>$file_name" or Terminate("Cannot create file $file_name\n");

}

sub CloseResultFile {
	close($RESULT_FILE_FD) if (defined($RESULT_FILE_FD));
}

sub PrintResultLine {
	my ($owner, $table, $partition, $line) = @_;
	my $tag;

	(defined($partition)) ? ($tag = "$owner.$table.$partition") : ($tag = "$owner.$table");

	if (defined($RESULTS_DIR)) {
		OpenResultFile($owner, $table, $partition) if (!defined($RESULT_FILE_FD));
		print $RESULTS_DIR $line;
	} else {
		PrintMsg($tag.' '.$line); 
	}
}

# FinalResults (
#		$diffs, #reference to DIFFS hash
#	);
sub FinalResults {
	my $owner = shift;
	my $table = shift;
	my $partition = shift;
	my $diffs = shift;
	my $tag;
	my ($k, $db);

	if (defined($partition)) {
		$tag = "$owner.$table.$partition";
	} else {
		$tag = "$owner.$table";
	}

	my %unique_keys;
	my @dbs = sort keys %{$diffs}; #list of databases/workers


	foreach $db (@dbs) { #for each worker/database stored in DIFFS hash
		foreach $k (keys %{$diffs->{$db}}) { #each key left in DIFFS hash for given database
			if (defined($unique_keys{$k})) {
				$unique_keys{$k}++;
			} else {
				$unique_keys{$k} = 0;
			}
		}
	}

	my $in_sync_counter=0; # should be 0, because they should have been already removed from DIFFS hash by workers or SecondStageLookup
	my $out_of_sync_counter=0;

	foreach $k (keys(%unique_keys)) { #for each key found in any database/worker output
		
		my $out_line = "OUT OF SYNC [$k] ";
		my $val;
		my $match = 1;

		foreach $db (@dbs) { #check all databases/workers output

			if ( defined($diffs->{$db}->{$k}) ) { #there is matching key stored

				if (not defined($val)) {

					$val = $diffs->{$db}->{$k};  #get value for comparison

				} elsif ($diffs->{$db}->{$k} ne $val) { #compare value from key for current db with previously stored val 

					$match = 0;

				}

				$out_line .= " $db: ".$diffs->{$db}->{$k};

			} else {

				$match = 0;

				$out_line .= " $db: missing ";
			}
		}

		if ($match) {
			$in_sync_counter++;
		} else {
			$out_of_sync_counter++;
			PrintResultLine($owner, $table, $partition, "$out_line\n");
		}

	}

	PrintMsg(WARNING, $tag." FinalResults(): in sync rows left in DIFFS table: $in_sync_counter \n") if ($in_sync_counter);
	PrintMsg($tag." FinalResults():  out of sync: $out_of_sync_counter\n");

	CloseResultFile();
}


1;
__END__
