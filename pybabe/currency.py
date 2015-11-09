# coding: utf-8
import urllib2
import json
from decimal import Decimal

## Implemented currencies descriptors
# USD
# EUR
# CHF

GOOGLE_EXCHANGE_RATES_API = 'http://rate-exchange.appspot.com/currency?from=%s&to=%s'
YAHOO_FINANCES_RATES_API = 'http://finance.yahoo.com/d/quotes.csv?s=%s%s=X&f=l1'

rates = {}

# Get rate once on an instance
def get_rate_with_cache(from_, to_):
    global rates
    if not from_ in rates:
        rates[from_] = {}
    if not to_ in rates[from_]:
        rates[from_][to_] = get_rate(from_, to_)
    return rates[from_][to_]

def get_rate(currency_from, currency_to):
    if currency_from == currency_to:
        return Decimal('1')

    try:
        return from_google(currency_from, currency_to)
    except Exception as e:
        try:
            return from_yahoo(currency_from, currency_to)
        except Exception as e:
            raise e

def from_google(currency_from, currency_to):
    url = GOOGLE_EXCHANGE_RATES_API % (currency_from, currency_to)
    response = urllib2.urlopen(url)
    body = json.load(response)
    if 'rate' in body:
        return Decimal(body['rate'])
    else:
        raise Exception('Failed to get exchange rate')

def from_yahoo(currency_from, currency_to):
    url = YAHOO_FINANCES_RATES_API % (currency_from, currency_to)
    response = urllib2.urlopen(url)
    rate = response.read().rstrip()
    if float(rate) == 0:
        raise Exception('Failed to get exchange rate')
    else:
        return Decimal(rate)


class USD(object):
    '''Descriptor for dollars'''

    def __init__(self, value=0):
        self.value = Decimal(value)
    def __get__(self, instance, owner):
        return self.value.quantize(Decimal(10) ** -6).normalize()
    def __set__(self, instance, value):
        self.value = Decimal(str(value))

class EUR(object):
    '''Descriptor for euros'''

    def __get__(self, instance, owner):
        return Decimal(instance.USD * get_rate_with_cache('USD', 'EUR')).quantize(Decimal(10) ** -6).normalize()
    def __set__(self, instance, value):
        instance.USD = Decimal(str(value)) * get_rate_with_cache('EUR', 'USD')

class CHF(object):
    '''Descriptor for franc suisse'''

    def __get__(self, instance, owner):
        return Decimal(instance.USD * get_rate_with_cache('USD', 'CHF')).quantize(Decimal(10) ** -6).normalize()
    def __set__(self, instance, value):
        instance.USD = Decimal(str(value)) * get_rate_with_cache('CHF', 'USD')


class Currency(object):
    '''Class to represent currency holding all descriptors'''
    USD = USD()
    EUR = EUR()
    CHF = CHF()
