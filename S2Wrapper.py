#!/usr/bin/env python

import errno
import os
import pty
import select
import signal
import subprocess
import re
import termios
import struct
import fcntl

import sys
import time
import signal
import readline, subprocess, re
import threading
from collections import deque
import ConfigParser
import stty
from threading import Timer
import string


class Savage2ConsoleHandler:

	def __init__(self):
		Savage2ConsoleHandler.queue = deque()
		Savage2ConsoleHandler.channel = []

	@staticmethod
	def addChannel (cb):
		Savage2ConsoleHandler.channel.append (cb)

	@staticmethod
	def delChannel (cb):
		Savage2ConsoleHandler.channel.remove (cb)

	@staticmethod
	def put (line):
		Savage2ConsoleHandler.queue.append (line)

	@staticmethod
	def broadcast(line=None):
		if line:
			Savage2ConsoleHandler.put(line)
		for line in Savage2ConsoleHandler.queue:
			for cb in Savage2ConsoleHandler.channel:
				cb(line)
		Savage2ConsoleHandler.queue.clear()


class Savage2DaemonHandler:

	def __init__(self):
		Savage2DaemonHandler.queue = deque()
		Savage2DaemonHandler.channel = []

	@staticmethod
	def addChannel (cb):
		Savage2DaemonHandler.channel.append (cb)

	@staticmethod
	def delChannel (cb):
		Savage2DaemonHandler.channel.remove (cb)

	@staticmethod
	def put (line):
		Savage2DaemonHandler.queue.append (line)

	@staticmethod
	def broadcast(line=None):
		if line:
			Savage2DaemonHandler.put(line)
		for line in Savage2DaemonHandler.queue:
			for cb in Savage2DaemonHandler.channel:
				cb(line)
		Savage2DaemonHandler.queue.clear()


class ConsoleParser:

	filters = []

	def __init__(self):
		self.filters = dict({
			# listed in order of appearance, likeliness and sanity
			self.onServerStatus  : re.compile ('SGame: Server Status: Map\((.*?)\) Timestamp\((\d+)\) Active Clients\((\d+)\) Disconnects\((\d+)\) Entities\((\d+)\) Snapshots\((\d+)\)'),
			self.onServerStatusResponse : re.compile ('Server Status: Map\((.*?)\) Timestamp\((\d+)\) Active Clients\((\d+)\) Disconnects\((\d+)\) Entities\((\d+)\) Snapshots\((\d+)\)'),
			self.onConnect     : re.compile ('Sv: New client connection: #(\d+), ID: (\d+), (\d+\.\d+\.\d+\.\d+):(\d+)'),
			self.onSetName     : re.compile ('Sv: Client #(\d+) set name to (\S+)'),
			self.onPlayerReady : re.compile ('Sv: Client #(\d+) is ready to enter the game'),
			self.onAccountId   : re.compile ('(?:SGame: |Sv: )*?Getting persistant stats for client (\d+) \(Account ID: (\d+)\)\.'),
			self.onStartServer : re.compile ('^K2 Engine start up.*'),
			self.onNewGameStarted : re.compile ('NewGameStarted'),
			#self.onConnected   : re.compile ('Sv: (\S+) has connected.'),
			self.onMessage     : re.compile ('Sv: \[(.+?)\] ([^\s]+?): (.*)'),
			self.onDisconnect  : re.compile ('SGame: Removed client #(\d+)'),
			self.onPhaseChange : re.compile ('(?:SGame: |Sv: )*?SetGamePhase\(\): (\d+) start: (\d+) length: (\d+) now: (\d+)'),
			self.onTeamChange  : re.compile ('(?:SGame: |Sv: )*?Client #(\d+) requested to join team: (\d+)'),
			self.onHasKilled   : re.compile ('Sv: (\S+) has been killed by (\S+)'),
			self.onUnitChange  : re.compile ('(?:SGame:|Sv:)?.?Client #(\d+) requested change to: (\S+)'),
			self.onCommResign  : re.compile ('SGame: (\S+) has resigned as commander.'),
			self.onMapReset    : re.compile ('.*\d+\.\d+\s{3, 6}'),
			# custom filters
			self.onPlaceBuilding : re.compile ('Sv: Place building: (\S+) at (\S+) (\S+) (\S+)'), 
			self.onItemTransaction : re.compile ('Sv: ITEM: Client (\d+) (\S+) (.*)'),
			self.onRefresh : re.compile ('^refresh'),
			self.onRefreshTeams : re.compile ('CLIENT (\d+) is on TEAM (\d+)'),
			self.onTeamCheck : re.compile ('^SERVER-SIDE client count, Team 1 (\d+), Team 2 (\d+)'),
			self.onRetrieveIndex : re.compile ('Sv: Client (\d+) index is (\d+). ACTION: (\S+)'),
			self.onListClients : re.compile ('^.* #(\d+) .*: (\d+\.\d+\.\d+\.\d+).*\\"(\S+)\"'),
			self.onGetLevels : re.compile ('^CLIENT (\d+) is LEVEL (\d+)'),
			self.RegisterStart : re.compile ('^STARTTOURNEY'),
			self.getNextDuel : re.compile ('^NEXTDUELROUND'),
			self.waitForPlayer : re.compile ('.*MISSING: (\S+)'),
			self.onDeath : re.compile('SGame: DUEL: (\d+) defeated (\d+)'),
			self.duelStarted : re.compile('SGame: DUEL_STARTED (\d+) (\d+) (\d+) (\S+) (\S+)'),
			self.duelEnded : re.compile('SGame: DUEL_ENDED (\d+) (\d+)'),
			self.onFighterRemoved : re.compile('SGame: REMOVED PLAYER (\d+) TEAM (\d+)'),
			self.getServerVar : re.compile('^SERVERVAR: (\S+) is (.*)'),
			self.getHashCheck : re.compile('Sv: HACKCHECK ClientNumber: (\d+), AccountID: (\d+), Hashcheck result: (\S+)'),
			self.getMatchID   : re.compile('SGame: Authenticated server successfully, stats will be recorded this game. Match ID: (\d+), Server ID: (\d+)'),
			self.getEvent : re.compile('(?:SGame: |Sv: )*?EVENT (\S+) (\S+) on (\S+) by (\S+) of type (\S+) at (\d+\.\d+) (\d+\.\d+) (\d+\.\d+)'),
			self.mapDimensions : re.compile('Error: CWorld::GetTerrainHeight\(\) - Coordinates are out of bounds'),
			self.onScriptEvent : re.compile('(?:SGame: |Sv: )*?SCRIPT Client (\d+) (\S+) with value (.*)')
		})

	def onLineReceived(self, line, dh):
		for handler in self.filters:
			filter = self.filters[handler]
			match = filter.match(line)
			if not match:
				continue

			try:
				handler(*match.groups(), Broadcast=dh)
			except Exception as e:
				print("Error in: %s: %s" % (repr(handler), e))


	# SGame: Server Status: Map(ss2010_6) Timestamp(69180000) Active Clients(9) Disconnects(160) Entities(1700) Snapshots(34671)
	def onServerStatus(self, *args, **kwargs):
		pass
	def onServerStatusResponse(self, *args, **kwargs):
		pass

	#X Sv: New client connection: #203, ID: 8625, 83.226.95.135:51427
	def onConnect(self, *args, **kwargs):
		pass

	#X Sv: Client #88 set name to Cicero23
	def onSetName(self, *args, **kwargs):
		pass

	def onPlayerReady(self, *args, **kwargs):
		pass

	def onAccountId(self, *args, **kwargs):
		pass

	def onStartServer(self, *args, **kwargs):
		pass

	def onNewGameStarted(self, *args, **kwargs):
		pass
	#def onConnected(self, *args, **kwargs):
	#	pass

	#X Sv: [TEAM 1] BeastSlayer`: need ammo
	#X Sv: [TEAM 2] BeastSlayer`: need ammo
	#X Sv: [ALL] bLu3_eYeS: is any 1 here ?
	def onMessage(self, *args, **kwargs):
		pass

	# SGame: Removed client #195
	def onDisconnect(self, *args, **kwargs):
		pass

	def onPhaseChange(self, *args, **kwargs):
		pass

	#X SGame: Client #180 requested to join team: IDX
	def onTeamChange(self, *args, **kwargs):
		pass

	def onHasKilled(self, *args, **kwargs):
		pass

	def onUnitChange(self, *args, **kwargs):
		pass

	def onCommResign(self, *args, **kwargs):
		pass
	def onMapReset(self, *args, **kwargs):
		print("SHUFFLE HAS BEEN CALLED, now DO SOMETHING")
		pass
	def onPlaceBuilding(self, *args, **kwargs):
		#f = open("/tmp/sav2game.log", "a+");
		#f.write();
		#f.close();
		pass

		
	# custom filters - TO BE REMOVED
	def onItemTransaction(self, *args, **kwargs):
		pass
	def onRefresh(self, *args, **kwargs):
		pass
	def onTeamCheck(self, *args, **kwargs):
		pass
	def onRefreshTeams(self, *args, **kwargs):
		pass
	def onRetrieveIndex(self, *args, **kwargs):
		pass
	def onListClients(self, *args, **kwargs):
		pass
	def onGetLevels(self, *args, **kwargs):
		pass
	def RegisterStart(self, *args, **kwargs):
		pass
	def getNextDuel(self, *args, **kwargs):
		pass
	def waitForPlayer(self, *args, **kwargs):
		pass
	def onDeath(self, *args, **kwargs):
		pass
	def duelStarted(self, *args, **kwargs):
		pass
	def duelEnded(self, *args, **kwargs):
		pass
	def onFighterRemoved(self, *args, **kwargs):
		pass
	def getServerVar(self, *args, **kwargs):
		pass
	def getHashCheck(self, *args, **kwargs):
		pass
	def getMatchID(self, *args, **kwargs):
		pass
	def getEvent(self, *args, **kwargs):
		pass
	def mapDimensions(self, *args, **kwargs):
		pass
	def onScriptEvent(self, *args, **kwargs):
		pass
	def cmd(self, string):
		Savage2DaemonHandler.broadcast(string)

class OutStream:
    def __init__(self, fileno):
        self._fileno = fileno
        self._buffer = ""

    def read_lines(self):
        try:
            output = os.read(self._fileno, 1000).decode()
        except OSError as e:
            if e.errno != errno.EIO: raise
            output = ""
        lines = output.split("\n")
        lines[0] = self._buffer + lines[0] # prepend previous
                                           # non-finished line.
        if output:
            self._buffer = lines[-1]
            return lines[:-1], True
        else:
            self._buffer = ""
            if len(lines) == 1 and not lines[0]:
                # We did not have buffer left, so no output at all.
                lines = []
            return lines, False

    def fileno(self):
        return self._fileno


def set_winsize(fd, row, col, xpix=0, ypix=0):
    winsize = struct.pack("HHHH", row, col, xpix, ypix)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, winsize)


ansi_regex = r'\x1b(' \
             r'(\[\??\d+[hl])|' \
             r'([=<>a-kzNM78])|' \
             r'([\(\)][a-b0-2])|' \
             r'(\[\d{0,2}[ma-dgkjqi])|' \
             r'(\[\d+;\d+[hfy]?)|' \
             r'(\[;?[hf])|' \
             r'(#[3-68])|' \
             r'([01356]n)|' \
             r'(O[mlnp-z]?)|' \
             r'(/Z)|' \
             r'(\d+)|' \
             r'(\[\?\d;\d0c)|' \
             r'(\d;\dR))'

sav2color_regex = r'\^[a-z]|\^\*'

ansi_escape = re.compile(ansi_regex, flags=re.IGNORECASE)
sav2color_escape = re.compile(sav2color_regex, flags=re.IGNORECASE)
stripped = lambda s: "".join(i for i in s if 31 < ord(i) < 127)


class Savage2Thread(threading.Thread):

	hack = 0
	process = None
	alive = False
	_relaunch = False
	def __init__(self, config):
		threading.Thread.__init__(self)
		self.config = config
		if self.config['once'] != "true":
			self._relaunch = True

	def run(self):		
		self.launchDaemon ()

	def launchDaemon (self):
		self.alive = True
		Savage2DaemonHandler.addChannel (self.onDaemonMessage)

		config = self.config
		args = [config['exec']]
		if config['args']:
			args += [config['args']]
                args[1] += ';Set sys_dedicatedConsole true;Set sys_debugOutput true'

		argenv = string.splitfields(config['env'], '=')

		print("starting: %s" % (args))
		sav2env = os.environ.copy()
		if len(argenv) == 2:
			sav2env[argenv[0].strip()] = argenv[1].strip()


		# Start the subprocess.
		self.out_r, out_w = pty.openpty()
		self.err_r, err_w = pty.openpty()
		set_winsize(self.out_r, 1000, 1000)
		set_winsize(self.err_r, 1000, 1000)

		self.process = subprocess.Popen(args, env=sav2env, stdin=subprocess.PIPE, stderr=err_w, stdout=out_w, universal_newlines=True)
		os.close(out_w) # if we do not write to process, close these.
		os.close(err_w)

		fds = {OutStream(self.out_r), OutStream(self.err_r)}

		print("[%s] has started successfully" % (self.process.pid))
		try:
			self.read(fds)
		except Exception as e:
			self.clean()
			if self._relaunch:
				self.relaunch()

	def read(self, fds):
            while self.alive and fds:
                # Call select(), anticipating interruption by signals.
                while self.alive:
                    try:
                        rlist, _, _ = select.select(fds, [], [])
                        break
                    except InterruptedError:
                        continue

                # Handle all file descriptors that are ready.
                for f in rlist:
                    lines, readable = f.read_lines()
                    # Example: Just print every line. Add your real code here.
                    for line in lines:
                        line = ansi_escape.sub('', line)
                        line = sav2color_escape.sub('', line)
                        if line == ">":
                            continue

                        Savage2ConsoleHandler.broadcast(stripped(line))

                    if not readable:
                        # This OutStream is finished.
			print("IOError: fileno:%s is closed." % f._fileno)
                        fds.remove(f)

		if not fds:
			print('relaunch')
			if self._relaunch:
				self.relaunch()

	def relaunch(self):
		# don't go crazy spawning process too fast, sleep some instead
		time.sleep(1.0)
		self.launchDaemon()

	def clean (self):
		os.close(self.out_r)
		os.close(self.err_r)

	def write(self, line):
		try:
			if self.process:
				self.process.stdin.write (line + "\n")
			else:
				print('foo')
		except IOError:
			self.clean()
			if self.relaunch:
				self.relaunch()

	def onSocketMessage (self, line):
		self.write (line)
	def onDaemonMessage (self, line):
		self.write (line)



# Launches various threads, actual savage2 daemon and the inet server
import PluginsManager
class Savage2Daemon:

	parser = None
	thread = None
	debug = ""
	dh = None

	def __init__(self, config):
		self.config = config

		# Setup our broadcasters
		Savage2ConsoleHandler ()
		self.dh = Savage2DaemonHandler ()

		# Add callback's for messages
		Savage2ConsoleHandler.addChannel (self.onConsoleMessage)

		# Launch savage2 thread
		# this thread will run and handle savage2 dedicated server
		# relaunching it on savage2.bin exit.
		self.thread = Savage2Thread(dict(config.items('core')))
		self.thread.daemon = True
		self.thread.start ()

	def exit(self):
		self.thread.alive = False
		self.thread.process.kill()

	def onConsoleMessage (self, line):
		print("%s" % line)
	
		for plugin in PluginsManager.getEnabled (PluginsManager.ConsoleParser):
			plugin.onLineReceived (line, self.dh)

		pass


def config_exists(name, suggestion=None):
	if os.path.isfile(name):
		return True
	msg = "ERROR: File not found: %s\n" % (name)
	sys.stderr.write(msg + suggestion + "\n")
	return False

def config_dump(config):
	for sect in config.sections():
		print(sect)
		for item in config.items(sect):
			print(item)
	return

def config_read(cfgs, config = None):
	if not config:
		config = ConfigParser.ConfigParser()

	print("config_read(): %s" % (cfgs))
	if config.read(cfgs):
		#config_dump(config)
		pass

	return config

def config_write(filename, config):
	print("config_write(): %s" % (filename))
	with open(os.path.expanduser(filename), "wb") as f:
		config.write(f)
	return



# Main thread
if __name__ == "__main__":
	# prepare paths and names
	path_install = os.path.dirname(os.path.realpath(__file__))
	path_plugins = os.path.join(path_install, "plugins")
	conf_def = "default.ini"
	conf_loc = "s2wrapper.ini"

	# read default config
	cfgdef = os.path.join(path_install, conf_def)
	if not config_exists(cfgdef, "Please get '%s' from upstream :)" % (conf_def)):
		sys.exit(1)
	config = config_read(cfgdef)

	# read local config
	path_home = config.get('core', 'home')
	if path_home == ".":
		path_home = path_install
	else:
		path_home = os.path.expanduser(path_home)
	#path_home = os.path.join(path_home, dir_mod)
	cfgloc    = os.path.join(path_home, conf_loc)
	if not config_exists(cfgloc, "Read '%s' for instructions" % (conf_def)):
		sys.exit(1)
	config_read(cfgloc, config)

	PluginsManager.discover(path_plugins)

	# enable listed plugins
	for name in config.options('plugins'):
		if name == 'admin':
			PluginsManager.enable(name)
			continue
		if config.getboolean('plugins', name):
			PluginsManager.enable(name)
		
	# launch daemon
	daemon = Savage2Daemon(config)

	# Catch keyboard interrupts, while we run our main while.
	try:
		while True:
			# block till user input
			try:
				line = raw_input("")
			except EOFError:
				print("%s: caught EOF, what should i do?" % (__name__))
				continue

			# get command and argument as 2 strings
			args = line.strip().split(None, 1)
			if not args:
				continue
			arg = args[1] if (len(args) > 1) else None
			cmd = args[0]

			if cmd == "exit":
				daemon.exit()
				break

			if cmd == "plugins":
				if not arg:
					print("Syntax: %s <command>" % (cmd))
					continue

				args = arg.split(None, 1)
				arg  = args[1] if (len(args) > 1) else None
				cmd2 = args[0]
				if not arg:
					if   cmd2 == "discover":
						PluginsManager.discover()
					elif cmd2 == "list":
						print("\n".join(PluginsManager.list()))
					else:
						print("%s: %s: unknown command" % (cmd, cmd2))
				else:
					if   cmd2 == "reload":
						PluginsManager.reload(arg)
					elif cmd2 == "enable":
						PluginsManager.enable(arg)
					elif cmd2 == "disable":
						PluginsManager.disable(arg)
					else:
						print("%s: %s: unknown command" % (cmd, cmd2))

			# pass rest through the broadcaster
			else:
				Savage2DaemonHandler.broadcast(line)

			pass
	except KeyboardInterrupt:
		print("%s: caught SIGINT" % (__name__))
		pass

	# Clean.
	print("%s: exiting..." % (__name__))
	daemon.exit()
