import pytest

from ldclient.config import Config
from ldclient.user_deduplicator import UserDeduplicator


user = { 'key': 'user1' }

def test_notice_user_returns_false_for_never_seen_user():
	ud = UserDeduplicator(Config())
	assert ud.notice_user(user) == False

def test_notice_user_returns_true_for_previously_seen_user():
	ud = UserDeduplicator(Config())
	ud.notice_user(user)
	assert ud.notice_user({ 'key': user['key'] }) == True

def test_oldest_user_forgotten_if_capacity_exceeded():
	ud = UserDeduplicator(Config(user_keys_capacity = 2))
	user1 = { 'key': 'user1' }
	user2 = { 'key': 'user2' }
	user3 = { 'key': 'user3' }
	ud.notice_user(user1)
	ud.notice_user(user2)
	ud.notice_user(user3)
	assert ud.notice_user(user3) == True
	assert ud.notice_user(user2) == True
	assert ud.notice_user(user1) == False

def test_user_becomes_new_again_each_time_we_notice_it():
	ud = UserDeduplicator(Config(user_keys_capacity = 2))
	user1 = { 'key': 'user1' }
	user2 = { 'key': 'user2' }
	user3 = { 'key': 'user3' }
	ud.notice_user(user1)
	ud.notice_user(user2)
	ud.notice_user(user1)
	ud.notice_user(user3)
	assert ud.notice_user(user3) == True
	assert ud.notice_user(user1) == True
	assert ud.notice_user(user2) == False
