"""aqualogic command line test app."""

import threading
import logging
import sys
import time
import argparse
import yaml

import paho.mqtt.client as mqtt

from aqualogic.core import AquaLogic
from aqualogic.states import States
from aqualogic.keys import Keys

logging.basicConfig(level=logging.WARNING)

_LOGGER = logging.getLogger("mqtt")
PANEL = AquaLogic()
CLIENT = mqtt.Client()

LAST_STATE = {
    "display": None,
    "pool_temp": None,
    "air_temp": None,
    "pump_speed": None,
    "pump_power": None,
    "CHECK_SYSTEM": None,
    "SPA": None,
    "POOL": None,
    "HEATER_1": None,
    "BLOWER": None,
    "LIGHTS": None,
    "FILTER": None,
    "HEATER_AUTO_MODE": None,
}
STATE_STR = {
    "CHECK_SYSTEM": States.CHECK_SYSTEM,
    "SPA":          States.SPA,
    "POOL":         States.POOL,
    "HEATER_1":     States.HEATER_1,
    "BLOWER":       States.BLOWER,
    "LIGHTS":       States.LIGHTS,
    "FILTER":       States.FILTER,
    "HEATER_AUTO_MODE": States.HEATER_AUTO_MODE,
    }


def on_connect(client, userdata, flags, rc):
    threading.current_thread().name = "mqtt"
    if rc != 0:
        if rc == 3:
            _LOGGER.error(
                "Unable to connect to MQTT server: MQTT Server unavailable"
            )
        elif rc == 4:
            _LOGGER.error(
                "Unable to connect to MQTT server: MQTT Bad username or password"
            )
        elif rc == 5:
            _LOGGER.error("Unable to connect to MQTT server: MQTT Not authorized")
        else:
            _LOGGER.error(
                "Unable to connect to MQTT server: Connection refused. Error code: "
                + str(rc)
            )

    _LOGGER.debug("MQTT connected")
    client.subscribe(f"aqualogic/command")
    client.publish("aqualogic/status/available", "ONLINE", retain=True)

LAST_MESSAGE = -10000

def on_message(client, userdata, msg):
  global LAST_MESSAGE
  LAST_MESSAGE = time.monotonic()
  LAST_STATE["display"] = None
  _LOGGER.info(f'Recieved mqtt: {msg}')
  command = msg.payload.decode('utf-8')
  PANEL.process_resume()
  try:
      if command == "HEATER_1":
        raise KeyError("Heater as key")
      new_state = States[command]
      _LOGGER.info(f'mqtt: new state {command}')
      PANEL.set_state(new_state, not PANEL.get_state(new_state))
  except KeyError:
    try:
        key = Keys[command]
        _LOGGER.info(f'mqtt: key {command}')
        PANEL.send_key(key)
    except KeyError:
        print(f'Invalid State/Key name in mqtt msg {msg.payload}')


LAST_UPDATE = 0
def _data_changed(panel):
  global LAST_UPDATE
  _LOGGER.info(f'Pool Temp: {panel.pool_temp} Air Temp: {panel.air_temp}')
  _LOGGER.info(f'Pump Speed: {panel.pump_speed} Pump Power: {panel.pump_power}')
  _LOGGER.info('States: {}'.format(panel.states()))
  if panel.get_state(States.CHECK_SYSTEM):
      _LOGGER.info('Check System: {}'.format(panel.check_system_msg))
  now = time.monotonic()
  updated =  (now + 240) >= LAST_UPDATE
  for k,v in LAST_STATE.items():
    try:
      if k in STATE_STR:
        pv = panel.get_state(STATE_STR[k])
      else:
        pv = getattr(panel, k)
    except:
      continue
    if pv == v:
      continue
    LAST_STATE[k] = pv
    _LOGGER.info(f'Panel: {k}: {pv}')
    updated = True
  # update all
  if updated:
    LAST_UPDATE = now
    for k,v in LAST_STATE.items():
      CLIENT.publish(f"aqualogic/status/{k}", v)

def _display_changed(text):
  if LAST_STATE["display"] == text:
    return
  LAST_STATE["display"] = text
  ntext = text[:20] + "\n" + text[20:]
  _LOGGER.info(f'Display:\n{ntext}')
  CLIENT.publish("aqualogic/status/display", ntext)

def cleanup():
    _LOGGER.error(f"Exception: exiting {e}")
    PANEL.stop()
    CLIENT.loop_stop(force=True)
    sys.exit(0)

def main():

  parser = argparse.ArgumentParser(description='Report status and press keys to/from mqtt.')
  parser.add_argument('--serial', metavar='dev_file', type=str, default="/dev/ttyUSB0",
                     help='path to serial interface')
  parser.add_argument('--status_interval', metavar='SECS', default=240,
      type=int, help='time to wait between update (unless mqtt msg is received; 0 - no sleeping)')
  parser.add_argument('--config', metavar='YAML_CONFIG_FILE',
    default="aqualogic.yaml", type=str, help='yaml configuration file')

  args = parser.parse_args()



  try:
      with open(args.config, "r") as yml:
          yargs = yaml.safe_load(yml)
      CLIENT.on_connect = on_connect
      CLIENT.on_message = on_message
      CLIENT.will_set(
          "aqualogic/status/available",
          payload="OFFLINE", qos=1, retain=True
      )
      if "mqtt" in yargs and "tls_crt" in yargs["mqtt"]:
        CLIENT.tls_set(yargs["mqtt"]["tls_crt"])
      if "mqtt" in yargs and "tls_insecure" in yargs["mqtt"]:
        CLIENT.tls_insecure_set(yargs["mqtt"]["tls_insecure"])
      mqtt_user = ""
      if "mqtt" in yargs and "user" in yargs["mqtt"]:
        mqtt_user = yargs["mqtt"]["user"]
      if "mqtt" in yargs and "password" in yargs["mqtt"]:
        mqtt_pw = yargs["mqtt"]["password"]
        CLIENT.username_pw_set(mqtt_user, password=mqtt_pw)
      else:
        CLIENT.username_pw_set(mqtt_user)
      while True:
        try:
          CLIENT.connect(yargs["mqtt"]["host"], yargs["mqtt"]["port"], 60)
          CLIENT.loop_start()
          break
        except Exception as e:
          _LOGGER.error(f"Unable to connect to MQTT server: {e}")
          time.sleep(10)
  except Exception as e:
      _LOGGER.error(f"Unable to connect to MQTT server: {e}")
      raise

  PANEL.connect_serial(args.serial)
  print('Connected!')

  READER_THREAD = threading.Thread(target=PANEL.process, args=[_data_changed,
    _display_changed])
  READER_THREAD.start()

  # periodic sampling ~ 5 messages on default menu at 6 seconds between plus
  # margin
  try:
    WAKE_TIME=48
    while True:
      if (args.status_interval <= 0):
        sleep_time = 2*WAKE_TIME
      else:
        sleep_time = args.status_interval
      PANEL.process_resume()
      if not READER_THREAD.is_alive():
        _LOGGER.error("PANEL is not running")
        raise Exception
      time.sleep(WAKE_TIME)
      sleep_time -= WAKE_TIME
      now = time.monotonic()
      if (args.status_interval > 0) and (now > (LAST_MESSAGE+180)):
        PANEL.process_pause()
        time.sleep(2)
        sleep_time -= 2
        CLIENT.publish("aqualogic/status/available", "SLEEPING")
      READER_THREAD.join(sleep_time)
    cleanup()
  except Exception as e:
    cleanup()


if __name__ == "__main__":
    main()


