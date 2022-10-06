"""aqualogic command line test app."""

import threading
import logging
import sys

import paho.mqtt.client as mqtt

from aqualogic.core import AquaLogic
from aqualogic.states import States

logging.basicConfig(level=logging.WARNING)


def _data_changed(panel):
    print(f'Pool Temp: {panel.pool_temp} Air Temp: {panel.air_temp}')
    print(f'Pump Speed: {panel.pump_speed} Pump Power: {panel.pump_power}')
    print('States: {}'.format(panel.states()))
    if panel.get_state(States.CHECK_SYSTEM):
        print('Check System: {}'.format(panel.check_system_msg))


if len(sys.argv) == 2:
    print('Connecting to {}...'.format(sys.argv[1]))
elif len(sys.argv) == 3:
    print('Connecting to {}:{}...'.format(sys.argv[1], sys.argv[2]))
else:
    print('Usage: cli [host] [port]')
    print('           [serial port]')
    quit()

PANEL = AquaLogic()
if len(sys.argv) == 2:
    PANEL.connect_serial(sys.argv[1])
else:
    PANEL.connect(sys.argv[1], int(sys.argv[2]))
print('Connected!')
print('To toggle a state, type in the State name, e.g. LIGHTS')

READER_THREAD = threading.Thread(target=PANEL.process, args=[_data_changed])
READER_THREAD.start()

while True:
    LINE = input()
    try:
        STATE = States[LINE]
        PANEL.set_state(STATE, not PANEL.get_state(STATE))
    except KeyError:
        print('Invalid State name {}'.format(LINE))
