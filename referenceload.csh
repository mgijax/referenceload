#!/bin/csh -f

#
# Wrapper script to create & load new reference associations
#
# Usage:  referenceload.csh
#

setenv CONFIGFILE $1

source ${CONFIGFILE}

rm -rf ${REFLOG}
touch ${REFLOG}

date >& ${REFLOG}

${REFERENCELOAD}/referenceload.py >>& ${REFLOG}

date >>& ${REFLOG}

