</$objtype/mkfile

TARG=raft
LIB=libraft/libraft.$O.a
BIN=/$objtype/bin

</sys/src/cmd/mkmany

$O.raft: raft.$O

servertest:V:	$O.agard
	@{
		rfork ne
		rm -f /srv/agar
		unmount /n/agar >[2]/dev/null || status=''
		broke|grep agard|rc
		kill $O.agard|rc
		$O.agard -D -d
		#srv tcp!$sysname!19000 agar /n/agar
		#ls /n/agar
		#exec cat /n/agar/event &
		#cpid=$apid
		#echo ping > /n/agar/ctl
		#echo kill > /proc/$cpid/ctl
		#rm -f /srv/agar
	}

CFLAGS=$CFLAGS -Ilibraft

$LIB:V:
	cd libraft
	mk

clean nuke:V:
	@{ cd libraft; mk $target }
	rm -f *.[$OS] [$OS].* $TARG raftlog.*
