# -*- coding: utf-8 -*-
# 3.25.11 - Added shuffle
import re
import math
import time
import ConfigParser
import threading
import random
from MasterServer import MasterServer
from PluginsManager import ConsolePlugin
from S2Wrapper import Savage2DaemonHandler
from operator import itemgetter

#This plugin was written by Old55 and he takes full responsibility for the junk below.
#He does not know python so the goal was to make something functional, not something
#efficient or pretty.


class admin(ConsolePlugin):
	VERSION = "1.0.1"
	playerlist = []
	adminlist = []
	banlist = []
	PHASE = 0

	def onPluginLoad(self, config):
		self.ms = MasterServer ()

		ini = ConfigParser.ConfigParser()
		ini.read(config)
		for (name, value) in ini.items('admin'):
			self.adminlist.append(name)
	def onStartServer(self, *args, **kwargs):
				
		self.playerlist = []
		self.banlist = []	

	def getPlayerByClientNum(self, cli):

		for client in self.playerlist:
			if (client['clinum'] == cli):
				return client

	def getPlayerByName(self, name):

		for client in self.playerlist:
			if (client['name'].lower() == name.lower()):
				return client

	def onConnect(self, *args, **kwargs):
		
		id = args[0]
		ip = args[2]
		print self.banlist
		print ip

		reason = "An administrator has removed you from this server. You may rejoin the server after the current game ends."
		
		for each in self.banlist:
			if each == ip:
				kwargs['Broadcast'].broadcast("Kick %s \"%s\"" % (id, reason))

		for client in self.playerlist:
			if (client['clinum'] == id):
				return
		
		self.playerlist.append ({'clinum' : id, 'acctid' : 0, 'name' : 'X', 'ip' : ip, 'team' : 0, 'sf' : 0, 'active' : False, 'level' : 0})
	
	def onDisconnect(self, *args, **kwargs):
		
		cli = args[0]
		client = self.getPlayerByClientNum(cli)
		client ['active'] = False

	def onSetName(self, *args, **kwargs):
		
		cli = args[0]
		playername = args[1]
		client = self.getPlayerByClientNum(cli)
		client ['name'] = playername
					
	def onAccountId(self, *args, **kwargs):
		cli = args[0]
		id = args[1]
		stats = self.ms.getStatistics (id).get ('all_stats').get (int(id))
		level = int(stats['level'])
		sf = int(stats['sf'])
					
		client = self.getPlayerByClientNum(cli)
		client['sf'] = sf
		client['level'] = level
		client['active'] = True	
		if self.isAdmin(client, **kwargs):
			kwargs['Broadcast'].broadcast("SendMessage %s ^cYou are registered as an administrator. Send the chat message: ^rhelp ^cto see what commands you can perform." % (cli))

		
	def isAdmin(self, client, **kwargs):
		admin = False
		
		for each in self.adminlist:
			if client['name'].lower() == each:
				admin = True
		
		return admin

	def onMessage(self, *args, **kwargs):
		
		name = args[1]
		message = args[2]
		
		client = self.getPlayerByName(name)
		admin = self.isAdmin(client, **kwargs)

		#ignore everything if it isn't from admin
		if not admin:
			return

		restart = re.match("restart", message, flags=re.IGNORECASE)
		shuffle = re.match("shuffle", message, flags=re.IGNORECASE)
		kick = re.match("kick (\S+)", message, flags=re.IGNORECASE)
		ban = re.match("ban (\S+)", message, flags=re.IGNORECASE)
		changeworld = re.match("changeworld (\S+)", message, flags=re.IGNORECASE)
		help = re.match("help", message, flags=re.IGNORECASE)
		prevphase = re.match("previous phase", message, flags=re.IGNORECASE)
		addadmin = re.match("admin (\S+) (\S+)", message, flags=re.IGNORECASE)
		balance = re.match("balance", message, flags=re.IGNORECASE)

		if restart:
			#restarts server if something catastrophically bad has happened
			kwargs['Broadcast'].broadcast("restart")

		if shuffle:
			#artificial shuffle vote
			self.onShuffle(client['clinum'], **kwargs)						
			
		if kick:
			#kicks a player from the server and temporarily bans that player's IP till the game is over
			reason = "An administrator has removed you from the server, probably for being annoying"
			kickclient = self.getPlayerByName(kick.group(1))
			kwargs['Broadcast'].broadcast("Kick %s \"%s\"" % (kickclient['clinum'], reason))
			
		if ban:
			#kicks a player from the server and temporarily bans that player's IP till the game is over
			reason = "An administrator has banned you from the server. You are banned till this game is over."
			kickclient = self.getPlayerByName(kick.group(1))
			kwargs['Broadcast'].broadcast("Kick %s \"%s\"" % (kickclient['clinum'], reason))
			self.banlist.append(kickclient['ip'])

		if changeworld:
			#change the map
			kwargs['Broadcast'].broadcast("changeworld %s" % (changeworld.group(1)))

		if prevphase:
			#if game is started, move back to previous phase
			if self.PHASE != 5:
				kwargs['Broadcast'].broadcast("SendMessage %s Cannot change phase if the game has not started!" % (client['clinum']))
				return

			kwargs['Broadcast'].broadcast("prevphase")

		if balance:
			if self.PHASE != 5:
				kwargs['Broadcast'].broadcast("SendMessage %s Cannot balance if the game has not started!" % (client['clinum']))
				return

			self.onBalance(client['clinum'], **kwargs)

		if addadmin:

			if client['name'] != "stony":
				return

			added = self.getPlayerByName(addadmin.group(2))

			if addadmin.group(1) == "add":
				self.adminlist.append(addadmin.group(2))
				
				kwargs['Broadcast'].broadcast("SendMessage %s You have added %s as a temporary administrator." % (client['clinum'], added['name']))
				kwargs['Broadcast'].broadcast("SendMessage %s You have been added as an administrator. Send the chat message: ^chelp ^wto see commands" % (added['clinum']))

			if addadmin.group(1) == "remove":
				for each in self.adminlist:
					if each == addadmin.group(2):
						self.adminlist.remove(each)
				kwargs['Broadcast'].broadcast("SendMessage %s You have removed %s as a temporary administrator." % (client['clinum'], added['name']))
				kwargs['Broadcast'].broadcast("SendMessage %s You have been removed as an administrator." % (added['clinum']))

		self.logCommand(client['name'],message)

		if help:
			kwargs['Broadcast'].broadcast("SendMessage %s All commands on the server are done through server chat. All commands are logged to prevent you from abusing them.The following are commands and a short description of what they do." % (client['clinum']))
			kwargs['Broadcast'].broadcast("SendMessage %s ^rrestart ^whard reset of the server. ONLY use in weird cases." % (client['clinum']))
			kwargs['Broadcast'].broadcast("SendMessage %s ^rshuffle ^wwill shuffle the game and set to previous phase." % (client['clinum']))
			kwargs['Broadcast'].broadcast("SendMessage %s ^rkick playername ^wwill remove a player from the server." % (client['clinum']))
			kwargs['Broadcast'].broadcast("SendMessage %s ^rban playername ^wwill remove a player from the server and ban that IP address till the end of the game." % (client['clinum']))
			kwargs['Broadcast'].broadcast("SendMessage %s ^rchangeworld mapname ^wwill change the map to the desired map." % (client['clinum']))
			kwargs['Broadcast'].broadcast("SendMessage %s ^rprevious phase ^wwill reset the map and set the game to the previous phase. May be better than changeworld if you are restarting a stacked game." % (client['clinum']))
			kwargs['Broadcast'].broadcast("SendMessage %s ^rbalance ^wwill move two players to achieve balance." % (client['clinum']))

			kwargs['Broadcast'].broadcast("SendMessage %s ^radmin add/remove playername ^wwill add or a remove a player to the admin list. Only stony can execute this." % (client['clinum']))
	def onPhaseChange(self, *args, **kwargs):
		phase = int(args[0])
		self.PHASE = phase

		if (phase == 7):
			self.banlist = []	
			for each in self.playerlist:
				each['team'] = 0

	def logCommand(self, client, message, **kwargs):
		localtime = time.localtime(time.time())
		date = ("%s-%s-%s, %s:%s:%s" % (localtime[1], localtime[2], localtime[0], localtime[3], localtime[4], localtime[5]))
		f = open('admin.log', 'a')		
		f.write("Timestamp: \"%s\", Admin: %s, Command: %s\n" % (date, client, message))
		f.close

	def onTeamChange (self, *args, **kwargs):
		
		team = int(args[1])
		cli = args[0]
		client = self.getPlayerByClientNum(cli)
		client['team'] = team

	def onShuffle (self, client, **kwargs):
		#Put all the active players in a list
		for each in self.playerlist:
			if not each['active']:
				continue
			if each['team'] > 0:
				shufflelist.append(each)
	
		#sort shufflelists based on SF
		shufflelist = sorted(shufflelist, key=itemgetter('sf', 'level', 'clinum'), reverse=True)
		
		#randomly choose if we begin with human or beast
		r = random.randint(1,2)
		
		#Assign new teams, just like the K2 way, but Ino won't always be on humans
		for each in shufflelist:
			
			each['team'] = r
			if r == 1:
				r += 1
			elif r == 2:
				r -=1
			
		#Now actually do the shuffling
		for each in shufflelist:
			kwargs['Broadcast'].broadcast("set _index #GetIndexFromClientNum(%s)#; SetTeam #_index# %s" % (each['clinum'], each['team']))
		#Finish it off by going back a phase
		kwargs['Broadcast'].broadcast("prevphase; prevphase; prevphase")
		kwargs['Broadcast'].broadcast("SendMessage %s You have shuffled the game." % (client))
		#Run balancer to get it nice and even
		self.onBalance(client, **kwargs)

	def onBalance(self, client, **kwargs):

		teamone = []
		teamtwo = []
		shufflelist = []
		combteamone = 0
		avgteamone = 0
		combteamtwo = 0
		avgteamtwo = 0
		gameavg = 0
		target = 0
		#populate current team lists:
		for each in self.playerlist:
			if not each['active']:
				continue
			if each['team'] == 1:
				teamone.append(each)
			if each['team'] == 2:
				teamtwo.append(each)

		#figure out current averages:
		for each in teamone:
			combteamone += each['sf']
		for each in teamtwo:
			combteamtwo += each['sf']
		
		sizeteamone = len(teamone)
		sizeteamtwo = len(teamtwo)
		avgteamone = combteamone/sizeteamone
		avgteamtwo = combteamtwo/sizeteamtwo
		gameavg = (avgteamone + avgteamtwo) / 2
		kwargs['Broadcast'].broadcast("SendMessage %s ^yPrior to balance: Team One Avg. SF was ^r%s^y, Team Two Avg. SF was ^r%s" % (client, avgteamone, avgteamtwo))

		target = abs(gameavg * (sizeteamone - avgteamone))

		lowest = -1
		pick1 = None
		pick2 = None
		eventeams = False
		teamonelarge = False

		if sizeteamone == sizeteamtwo:
			eventeams = True
			teamonelarge = False

		if sizeteamone > sizeteamtwo:
			teamonelarge = True

		for player1 in teamone:
			for player2 in teamtwo:
								
				ltarget = abs(player1['sf'] - player2['sf'] + target)
				
				if (lowest < 0):
					lowest = ltarget
					pick1 = player1
					pick2 = player2
					continue
			
				if (lowest < ltarget):
					continue
			
				lowest = ltarget
				pick1 = player1
				pick2 = player2

		if not eventeams and teamonelarge:
			t1 = -1
			t2 = 1

		if not eventeams and not teamonelarge:
			t1 = 1
			t2 = -1

		diff = abs(avgteamone - avgteamtwo)
		team1SF = combteamone - pick1['sf'] + pick2['sf']
		team2SF = combteamtwo - pick2['sf'] + pick1['sf']
		newdiff = abs((team1SF / (sizeteamone + t1)) - (team2SF / (sizeteamtwo + t2)))

		#If the avg isn't improved, abort it
		if (newdiff >= diff):
			print 'unproductive balance. terminate'
			kwargs['Broadcast'].broadcast("echo unproductive EVEN balance")
			return
		#Do the switch
		kwargs['Broadcast'].broadcast("set _index #GetIndexFromClientNum(%s); SetTeam #_index# 2; set _index #GetIndexFromClientNum(%s); SetTeam #_index# 1" % (pick1['clinum'], pick2['clinum']))
		
