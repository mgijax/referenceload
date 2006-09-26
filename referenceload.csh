#!/bin/csh -f

#
# Wrapper script to create & load new reference associations
#
# Usage:  referenceload.csh
#

setenv CONFIGFILE $1

cd `dirname $0` && source ${CONFIGFILE}

setenv REFLOG	$0.log
rm -rf ${REFLOG}
touch ${REFLOG}

date >& ${REFLOG}

${REFERENCELOAD}/referenceload.py >>& ${REFLOG}

date >>& ${REFLOG}

