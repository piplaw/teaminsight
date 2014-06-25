#### MASTER UCP SCRIPT - CRAIG FITCHES v0.1 ####
use strict;
use warnings;
use File::Copy;
use POSIX qw(strftime);

#### Change Variables ###

my $rootDirectory = "\\\\lon5nas101\\bandq\\logs\\input\\kingftradepointprodUCP\\";
my $inputDirectory = "\\\\lon5nas101\\bandq\\logs\\input\\kingftradepointprod\\";

#DO NOT CHANGE ANYTHING BELOW THIS LINE#

#my $inputDirectory = $rootDirectory."1input\\";;
my $unzipDirectory = $rootDirectory."2unzip\\";
my $lookupDirectory = $rootDirectory."3lookup\\";
my $outputDirectory = $rootDirectory."4output\\";
my $doneDirectory = $rootDirectory."4output\\";
my $logDirectory = $rootDirectory."log\\";
my $archiveDirectory = $lookupDirectory."archive\\";

my @doneFiles = ScanDirectory($doneDirectory,'');


#STAGE 1 -  Unzip the SiteCat Files
my @inputFiles = ScanDirectory($inputDirectory,'.tsv.gz');

foreach(@inputFiles)
{
	next unless $_ =~ /(.*)tsv.gz$/;
	next unless checkExists($_) == 0;
	print "Processing File: ".$_."\n";
	gzipFile($inputDirectory.$_,$unzipDirectory.$_);
	print "Finished Unzipping File: ".$_."\n";
}

print "Stage 1 Unzip - Complete\n";

#STAGE 2 - Create Lookup File

my @lookupFiles = ScanDirectory($unzipDirectory,'txt');

open (OUTFILE, '>>', $lookupDirectory.'sc_cust_lookup.txt');
foreach (@lookupFiles)
{
	next unless checkExists($_) == 0;
	open (INFILE, $unzipDirectory.$_);

	while (<INFILE>) {
		chomp;
		my @f = split("\t");
		if(length($f[245])){
			print OUTFILE "$f[8]-$f[9]\t$f[245]\n";
		}
	}
	close (INFILE);
}
close (OUTFILE);


print "Stage 2 Lookup - Complete\n";

#STAGE 3 - Dedupe Lookup File

open (TODEDUPE,$lookupDirectory.'sc_cust_lookup.txt');
open (OUTFILE,'>', $lookupDirectory.'sc_cust_lookup_dedupe.txt');

my %seen = ();
{
	while(<TODEDUPE>)
	{
		$seen{$_}++;
		next if $seen{$_} > 1;
		print OUTFILE;
	}
}

close (TODEDUPE);
close (OUTFILE);

print "Stage 3 Dedupe Lookup - Complete\n";

#STAGE 4 - Add to SiteCat Files

my @unzipFiles = ScanDirectory($unzipDirectory,'.txt');

#create HASH of lookup
open (LOOKUP, $lookupDirectory.'sc_cust_lookup_dedupe.txt');
my %hash;
while (<LOOKUP>)
{
   chomp;
   my ($key, $val) = split /\t/;
   $hash{$key} .= $val;
}
close (LOOKUP);

while(@unzipFiles)
{
	my $file = shift(@unzipFiles);
	next unless checkExists($file) == 0;
	print "Adding ID to SC File: ".$file."\n";
	
	open (INFILE, $unzipDirectory.$file);
	open (OUTFILE, '>', $outputDirectory.$file);

	while (<INFILE>) {
		chomp;
		my $row = $_;
		my @f = split("\t");
		my $scCookie = "$f[8]-$f[9]";
		my $id = $hash{$scCookie};
		#print "ID:".$id."\n";
		
		if($id){
			print OUTFILE "$row\t$id\n";
		} else {
			print OUTFILE "$row\t\n";
		}
	}
	close (INFILE);
	close (OUTFILE);
}
	

#### CLEAN UP #####
#archive the lookup.
my $date = strftime "%m-%d-%Y", localtime;
move($lookupDirectory.'sc_cust_lookup.txt',$archiveDirectory.$date.'_sc_cust_lookup.txt') or die "Move failed: $!";
rename($lookupDirectory.'sc_cust_lookup_dedupe.txt',$lookupDirectory.'sc_cust_lookup.txt',);

print "All Stages Complete\n";
exit;

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # #  FUNCTIONS # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

sub gzipFile {
  my $inFile = shift;
  my $outFile = shift;
  
  system("C:\\GnuWin32\\bin\\gzip.exe -dc $inFile > $outFile.txt");
  if ($? == -1) {
    print "failed to execute unzip: $!\n";
  }
}

sub ScanDirectory{
    my ($workdir) = shift;
	my ($extension) = shift;
    #chdir($workdir) or die "Unable to enter dir $workdir\n $!\n";
    opendir(DIR, $workdir) or die "Unable to open $workdir\n $!\n";
    my @names = readdir(DIR) or die "Unable to read $workdir\n $!\n";
    closedir(DIR);
    my @return = "";

    foreach my $name (@names){
	# Use a regular expression to ignore files beginning with a period
       next if ($name =~ m/^\./);
       next unless $name =~ /(.*$extension$)/;
		  #print $name."\n";
		  push(@return, $name);
    }
	return @return;
}

sub checkExists
{
	my $input = shift;
	my $output = "";
	
	#set the input to ignore any file extensions
	if ($input =~ m/(\w*-\w*)/)
	{
		$input = $1;
	}
	
	foreach (@doneFiles){
		#print "INPUT: ".$input."\n";
		#print "LOOP: ".$_."\n";
		
		if ($_ =~ m/(\w*-\w*)/)
		{
			$output = $1;
		}
		
		 if ($input eq $output){
			 #file exists
			 return 1;
		 }
	}
	
	return 0;
	
}

sub addToLog
{
	my ($logrow) = shift;
	my $timestamp = scalar localtime();
	open OUT, '>>', $logDirectory."Log.txt" or die;
	print OUT  $timestamp." - ".$logrow."\n";
	close OUT;	
}