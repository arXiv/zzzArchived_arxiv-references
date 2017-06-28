#!/bin/bash

VERSION=v0.906.0
PACKAGE=TeX-AutoTeX-${VERSION}

rm -r perllib/TeX
wget http://search.cpan.org/CPAN/authors/id/T/TS/TSCHWAND/$PACKAGE.tar.gz
tar xvf $PACKAGE.tar.gz
patch -p0 < $PACKAGE.patch
mv $PACKAGE/lib/TeX perllib

rm -r $PACKAGE
rm -r $PACKAGE.tar.gz
