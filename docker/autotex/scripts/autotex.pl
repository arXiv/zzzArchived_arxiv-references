#!/usr/bin/perl

use lib '/perllib/';
use TeX::AutoTeX;
#my $t = TeX::AutoTeX->new( workdir => '/tmp/autotex',);
#$t->process or warn 'processing failed';

# use TeX::AutoTeX;
print 'hi';
my $t = TeX::AutoTeX->new( workdir => '/autotex/', verbose => 1,);
$t->{log} = TeX::AutoTeX::Log->new(
                dir     => $t->{workdir},
                verbose => $t->{verbose},
                #dupefh  => $t->{verbose} ? \*STDOUT : undef,
            );

$t->{log}->open_logfile();
$t->set_dvips_resolution(600);
#$t->set_stampref(['foobar paper', 'http://example.com/my/paper.pdf']);
if ($t->process()) {
    print "Success\n";
} else {
    print "Processing failed: inspect $t->{workdir}/auto_gen_ps.log\n";
}
