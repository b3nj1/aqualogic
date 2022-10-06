# -*- coding: utf-8 -*-
"""A library to interface with a Hayward/Goldline AquaLogic/ProLogic
pool controller."""

from enum import IntEnum, unique
from threading import Timer, Thread, Event, Lock
import binascii
import logging
import queue
import socket
import time
import serial
import datetime
import dlestxetx
from copy import deepcopy
from collections import deque

from .web import WebServer
from .states import States
from .keys import Keys

_LOGGER = logging.getLogger(__name__)


class AquaLogic():
    """Hayward/Goldline AquaLogic/ProLogic pool controller."""

    # pylint: disable=too-many-instance-attributes
    FRAME_DLE = 0x10
    FRAME_STX = 0x02
    FRAME_ETX = 0x03

    READ_TIMEOUT = 5
    WATCH_FRAMES = 0

    frame_recurring = {}
    frame_names = {}
    # Local wired panel (black face with service button)
    FRAME_TYPE_LOCAL_WIRED_KEY_EVENT = b'\x00\x02'
    frame_names[0x2] = 'local wired key'
    # Remote wired panel (white face)
    FRAME_TYPE_REMOTE_WIRED_KEY_EVENT = b'\x00\x03'
    frame_names[0x3] = 'remote wired key'
    # Wireless remote
    FRAME_TYPE_WIRELESS_KEY_EVENT = b'\x00\x83'
    frame_names[0x83] = 'wireless key'
    FRAME_TYPE_ON_OFF_EVENT = b'\x00\x05'   # Seems to only work for some keys
    frame_names[0x05] = 'on/off'

    FRAME_TYPE_KEEP_ALIVE = b'\x01\x01'
    frame_names[0x0101] = 'KA'
    frame_recurring[0x0101] = True
    FRAME_TYPE_LEDS = b'\x01\x02'
    frame_names[0x0102] = 'LEDS'
    frame_recurring[0x0102] = True
    FRAME_TYPE_DISPLAY_UPDATE = b'\x01\x03'
    frame_names[0x0103] = 'display'
    frame_recurring[0x0103] = True
    FRAME_TYPE_LONG_DISPLAY_UPDATE = b'\x04\x0a'
    frame_names[0x040a] = 'long display'
    frame_recurring[0x040a] = True
    FRAME_TYPE_PUMP_SPEED_REQUEST = b'\x0c\x01'
    frame_names[0x0c01] = 'pump speed'
    frame_recurring[0x0c01] = True
    FRAME_TYPE_PUMP_STATUS = b'\x00\x0c'
    frame_names[0x000c] = 'pump status'
    frame_recurring[0x000c] = True
    frame_recurring[0x0407] = True
    frame_recurring[0x0004] = True
    frame_recurring[0x04a0] = True
    frame_recurring[0x04ae] = True
    # others
    # WARNING:aqualogic.core:dlestxetx df FT:b'\x04\x07' "" "" B:b'\x04\x07'
    # WARNING:aqualogic.core:dlestxetx df FT:b'\x00\x04' "" "" B:b'\x00\x04\x02'
    # WARNING:aqualogic.core:dlestxetx df FT:b'\x04\xae' "®" "not utf-8" B:b'\x04\xae'

    def __init__(self, web_port=8129):
        self._stop = Event()
        self._run = Event()
        self._run.set()
        self._send_now = Event()
        self._send_ack = Event()
        self._socket = None
        self._serial = None
        self._io = None
        self._is_metric = False
        self._send_queue = queue.Queue()
        self._multi_speed_pump = False
        self._air_temp = None
        self._pool_temp = None
        self._spa_temp = None
        self._pool_chlorinator = None
        self._spa_chlorinator = None
        self._salt_level = None
        self._check_system_msg = None
        self._pump_speed = None
        self._pump_power = None
        self._states = 0
        self._sleep_time = 0
        self._sleep_since = None
        self._flashing_states = 0
        self._heater_auto_mode = True  # Assume the heater is in auto mode
        self._watch_frames = None
        self._kp_display_trace = deque()
        for i in range(2):
          self._kp_display_trace.append(i)
        self._kp_pending_events = dict()
        self._kp_pending_histogram = open("key_logger", "a", buffering=1)

        self._write_thread = Thread(target=self._send_frame_thread)
        self._write_thread.start()

        self._web = None
        if web_port and web_port != 0:
            # Start the web server
            self._web = WebServer(self)
            self._web.start(web_port)

    def stop(self):
      self._kp_pending_histogram.close()
      self._stop.set()

    def connect(self, host, port):
        self.connect_socket(host, port)

    def connect_socket(self, host, port):
        """Connects via a RS-485 to Ethernet adapter."""
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._socket.settimeout(self.READ_TIMEOUT)
        self._read = self._read_byte_from_socket
        self._write = self._write_to_socket

    def connect_serial(self, serial_port_name):
        self._serial = serial.Serial(port=serial_port_name, baudrate=19200,
                          stopbits=serial.STOPBITS_TWO, timeout=self.READ_TIMEOUT)
        self._read = self._read_byte_from_serial
        self._write = self._write_to_serial
    
    def connect_io(self, io):
        self._io = io
        self._read = self._read_byte_from_io
        self._write = self._write_to_io

    def _check_state(self, data):
        desired_states = data['desired_states']
        for desired_state in desired_states:
            if (self.get_state(desired_state['state']) !=
                    desired_state['enabled']):
                # The state hasn't changed
                data['retries'] -= 1
                if data['retries'] > 0:
                    # Re-queue the request
                    _LOGGER.info('requeue')
                    self._send_queue.put(data)
                    return
            else:
                _LOGGER.debug('state change successful')

    def _read_byte_from_socket(self):
        data = self._socket.recv(1)
        return data[0]
    
    def _read_byte_from_serial(self):
        data = self._serial.read(1)
        if len(data) == 0:
            raise serial.SerialTimeoutException()
        return data[0]
        
    def _read_byte_from_io(self):
        data = self._io.read(1)
        if len(data) == 0:
            raise EOFError()
        return data[0]
    
    def _write_to_socket(self, data):
        self._socket.send(data)
    
    def _write_to_serial(self, data):
        self._serial.write(data)
        self._serial.flush()
        
    def _write_to_io(self, data):
        self._io.write(data)
        
    def _send_frame_thread(self):
        while not self._stop.is_set():
            data = self._send_queue.get()
            frame = data['frame']

            tries = 1
            self._send_now.clear()
            while tries < 4:
              self._watch_frames = self.WATCH_FRAMES+3
              self._send_now.wait()
              self._send_ack.clear()
              self._write(frame)
              now = time.monotonic()
              self._kp_pending_events[now] = ["send", frame] + list(self._kp_display_trace)
              _LOGGER.info('%3.3f: Sent: %s', now,
                           binascii.hexlify(frame))
              if self._send_ack.wait(timeout=0.25):
                _LOGGER.info(f'got ack {tries}')
                break
              tries = tries + 1
              self._send_now.clear()



            try:
                if data['desired_states'] is not None:
                    # Set a timer to verify the state changes
                    # Wait 2 seconds as it can take a while for
                    # the state to change.
                    Timer(3.0, self._check_state, [data]).start()
            except KeyError:
                pass
            if not self._send_queue.empty():
              _LOGGER.warning('%3.3f: Sent but %d more to send', time.monotonic(),
                self._send_queue.qsize())

    def _send_frame(self):
        if not self._send_queue.empty():
            data = self._send_queue.get(block=False)
            t = time.monotonic()
            for i in range(1,2):
              self._write(data['frame'])
              nt = time.monotonic()
              st = 0.008 - (nt-t)
              if st > 0:
                time.sleep(st)
              t = nt

            _LOGGER.info('%3.3f: Sent: %s', time.monotonic(),
                         binascii.hexlify(data['frame']))

            try:
                if data['desired_states'] is not None:
                    # Set a timer to verify the state changes
                    # Wait 2 seconds as it can take a while for
                    # the state to change.
                    Timer(2.0, self._check_state, [data]).start()
            except KeyError:
                pass
            if not self._send_queue.empty():
              _LOGGER.warning('%3.3f: Sent but %d more to send', time.monotonic(),
                self._send_queue.qsize())

    def process_pause(self):
        #return # disable pause
        self._sleep_since = time.monotonic()
        _LOGGER.info('Sleeping process')
        self._run.clear()
        self._send_now.clear()

    def process_resume(self):
        now = time.monotonic()
        if self._sleep_since is not None:
          self._sleep_time += (now - self._sleep_since)
        self._sleep_since = now
        _LOGGER.info('Waking process')
        self._run.set()

    def kp_display_update(self, frame):
        self._kp_display_trace.append(frame)
        self._kp_display_trace.popleft()
        full_len = len(self._kp_display_trace)*2 + 2
        done_list = list()
        for t,kp in self._kp_pending_events.items():
          kp.append(frame)
          if len(kp) == full_len:
            done_list.append(t)
        for t in done_list:
          self._kp_pending_histogram.write(str(self._kp_pending_events[t]))
          self._kp_pending_histogram.write("\n")
          del self._kp_pending_events[t]

    def process(self, data_changed_callback, display_callback = None):
        """Process data; returns when the reader signals EOF.
        Callback is notified when any data changes."""
        # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        keep_alive_time = None
        count_keep_alive_time = 0
        sum_keep_alive_time = 0
        min_keep_alive_time = 1e9
        max_keep_alive_time = 0
        send_ready_count = 0
        last_send_ready_count = 0
        def threaded_cb():
          if data_changed_callback:
              Thread(target=data_changed_callback, args=[self]).start()

        frame_counts = {}
        frame_intervals = {}
        frame_unknown_counts = {}
        stats_time = time.monotonic()
        start_time = stats_time
        frame_reference = None
        watch_trace = deque()
        watch_long_display = None
        last_long_display = None
        try:
            while not self._stop.is_set():
                self._run.wait()
                self._send_now.clear()
                # Data framing (from the AQ-CO-SERIAL manual):
                #
                # Each frame begins with a DLE (10H) and STX (02H) character start
                # sequence, followed by a 2 to 61 byte long Command/Data field, a
                # 2-byte Checksum and a DLE (10H) and ETX (03H) character end
                # sequence.
                #
                # The DLE, STX and Command/Data fields are added together to
                # provide the 2-byte Checksum. If any of the bytes of the
                # Command/Data Field or Checksum are equal to the DLE character
                # (10H), a NULL character (00H) is inserted into the transmitted
                # data stream immediately after that byte. That NULL character
                # must then be removed by the receiver.

                byte = self._read()
                frame_start_time = None

                frame_rx_time = datetime.datetime.now()

                full_frame = bytearray()

                while True:
                    # Search for FRAME_DLE + FRAME_STX
                    full_frame = bytearray()
                    if byte == self.FRAME_DLE:
                        full_frame.append(byte)
                        frame_start_time = time.monotonic()
                        next_byte = self._read()
                        full_frame.append(next_byte)
                        if next_byte == self.FRAME_STX:
                            break
                        else:
                            continue
                    byte = self._read()
                    elapsed = datetime.datetime.now() - frame_rx_time
                    if elapsed.seconds > self.READ_TIMEOUT:
                        _LOGGER.info('Frame timeout')
                        return

                frame = bytearray()
                byte = self._read()

                while True:
                    full_frame.append(byte)
                    if byte == self.FRAME_DLE:
                        # Should be FRAME_ETX or 0 according to
                        # the AQ-CO-SERIAL manual
                        next_byte = self._read()
                        full_frame.append(next_byte)
                        if next_byte == self.FRAME_ETX:
                            break
                        elif next_byte != 0:
                            # Error?
                            pass

                    frame.append(byte)
                    byte = self._read()

                if False and int.from_bytes(frame[0:2], byteorder='big') == 0x0101:
                  self._send_now.set()

                if self.WATCH_FRAMES > 0: # stats on message arrivals
                  try:
                    now = time.monotonic()
                    ff_str = binascii.hexlify(full_frame)
                    ft = int.from_bytes(full_frame[2:4], byteorder='big')
                    msg_bytes = full_frame[4:-4]
                    if ft in self.frame_names:
                      wt_ft = self.frame_names[ft]
                    else:
                      wt_ft = "unknown"
                    watch_trace.append( (now, wt_ft, ff_str) )
                    if len(watch_trace) > self.WATCH_FRAMES:
                      watch_trace.popleft()
                    if wt_ft == "long display":
                      if msg_bytes != watch_long_display:
                        nstr = binascii.hexlify(msg_bytes)
                        try:
                          ostr = binascii.hexlify(watch_long_display)
                        except:
                          ostr = "<decerr>"
                        # _LOGGER.warning(f'dlestxetx LD change: \n   {ostr}\n      {watch_long_display}\n   {nstr}\n      {msg_bytes}')
                        watch_long_display = msg_bytes

                    if self._watch_frames and self._watch_frames >= 0:
                      _LOGGER.warning(f'dlestxetx watch {self._watch_frames:3d} {now:10.5f} {wt_ft:15s} {ff_str}')
                      watch_trace.popleft()
                      self._watch_frames = self._watch_frames - 1
                    # strip CRC
                    try:
                      decoded_ff = dlestxetx.decode(full_frame)[:-2]
                    except Exception as e:
                      _LOGGER.error(f'dlestxetx decode exception {e}  FF: {len(full_frame)} {full_frame}')
                    try:
                      ft = int.from_bytes(decoded_ff[0:2], byteorder='big')
                    except Exception as e:
                      _LOGGER.warning(f'dlestxetx ft exception {e} {full_frame}')
                    if ft in self.frame_recurring:
                      logl =  _LOGGER.debug
                    else:
                      logl =  _LOGGER.warning
                      logl(f'dlestxetx watch STR {now:10.5f} {wt_ft:15s} {ff_str}')
                      self._watch_frames = 0 - len(watch_trace)
                      while watch_trace:
                        (i_wt_t, i_wt_ft, i_wt_ff) = watch_trace.popleft()
                        _LOGGER.warning(f'dlestxetx watch {self._watch_frames:3d} {i_wt_t:10.5f} {i_wt_ft:15s} {i_wt_ff}')
                        self._watch_frames = self._watch_frames + 1
                      logl(f'dlestxetx watch *** {now:10.5f} {wt_ft:15s} {ff_str}')
                      self._watch_frames = self.WATCH_FRAMES


                    decoded_ff = decoded_ff[2:]
                    try:
                      try:
                        if ft not in frame_counts:
                          frame_counts[ft] = 0
                        frame_counts[ft] += 1
                      except:
                        _LOGGER.error(f'dlestxetx ft -1 {ft} oops')
                      try:
                        if ft not in self.frame_names:
                          if ft not in frame_unknown_counts:
                            frame_unknown_counts[ft] = {}
                          if decoded_ff not in frame_unknown_counts[ft]:
                            frame_unknown_counts[ft][decoded_ff] = 0
                          frame_unknown_counts[ft][decoded_ff] += 1
                      except Exception as e:
                        _LOGGER.error(f'dlestxetx UK -1 {ft} oops {e}')

                      if ft not in frame_intervals:
                        frame_intervals[ft] = {"last": now, "hist": {}}
                      if ft in self.frame_recurring:
                        precision = 10.0
                      else:
                        precision = 100.0
                        frame_intervals[ft]["last"] = frame_reference

                      if frame_intervals[ft]["last"] != now:
                        i = round(precision*(now - frame_intervals[ft]["last"]))/precision
                        if i not in frame_intervals[ft]["hist"]:
                          frame_intervals[ft]["hist"][i] = 0
                        frame_intervals[ft]["hist"][i] += 1
                        frame_intervals[ft]["last"] = now

                      if ft == 0x0101:
                        frame_reference = now

                      if (now - stats_time) >= 30:
                        try:
                          for t,hc in frame_intervals.items():
                            try:
                              fn = self.frame_names[t]
                            except:
                              fn = "unknown"
                            for i,c in frame_intervals[t]["hist"].items():
                              logl(f'dlestxetx IV:{fn:15s} {t:04x} {i:9.3f} {c:5d}')
                        except:
                              _LOGGER.error(f'dlestxetx IV oops')
                        for t,c in frame_counts.items():
                          try:
                            fn = self.frame_names[t]
                          except:
                            fn = "unknown"
                          try:
                            rate = float(c)/(now - start_time - self._sleep_time)
                            interval = 1.0/rate
                            logl(f'dlestxetx FT:{t:04x} {c:5d} {interval:9.5f} sec {rate:7.5f}/sec {fn}')
                          except:
                            _LOGGER.error(f'dlestxetx ft 1 oops')
                        try:
                          for t,dc in frame_unknown_counts.items():
                            for d,c in frame_unknown_counts[t].items():
                              logl(f'dlestxetx UK:{t:04x} {c:5d} {d}')
                        except:
                              _LOGGER.eorr(f'dlestxetx UK oops')

                        stats_time = now
                    except:
                      _LOGGER.error(f'dlestxetx ft 2 oops')

                    if frame_counts[ft] == 1:
                      try:
                        decoded_ff_lstr = decoded_ff.decode('Latin-1')
                      except:
                        decoded_ff_lstr = 'not latin'
                      try:
                        decoded_ff_ustr = decoded_ff.decode('utf-8')
                      except:
                        decoded_ff_ustr = 'not utf-8'
                      _LOGGER.warning(f'dlestxetx df FT:{ft:04x} "{decoded_ff_lstr}" "{decoded_ff_ustr}" B:{decoded_ff}')
                  except Exception as e:
                    _LOGGER.error(f'dlestxetx exception {e} {full_frame}')

                # Verify CRC
                frame_crc = int.from_bytes(frame[-2:], byteorder='big')
                frame = frame[:-2]

                calculated_crc = self.FRAME_DLE + self.FRAME_STX
                for byte in frame:
                    calculated_crc += byte

                if frame_crc != calculated_crc:
                    _LOGGER.info(f'Bad CRC {frame}')
                    continue

                frame_type = frame[0:2]
                frame = frame[2:]

                if frame_type != self.FRAME_TYPE_KEEP_ALIVE:
                  last_send_ready_count = send_ready_count
                  send_ready_count = 0

                if frame_type == self.FRAME_TYPE_KEEP_ALIVE:
                    new_keep_alive_time = time.monotonic()
                    if keep_alive_time:
                      delta = new_keep_alive_time - keep_alive_time
                      sum_keep_alive_time += delta
                      count_keep_alive_time += 1
                      if delta > max_keep_alive_time:
                        max_keep_alive_time = delta
                      if delta < min_keep_alive_time:
                        min_keep_alive_time = delta
                      #_LOGGER.warning('%3.3f: KA: src: %d interval: %3.5f : %3.5f <> %3.5f <> %3.5f', frame_start_time, send_ready_count, delta, min_keep_alive_time, sum_keep_alive_time/count_keep_alive_time, max_keep_alive_time)
                    # Keep alive
                    # _LOGGER.debug('%3.3f: KA', frame_start_time)
                    keep_alive_time = time.monotonic()

                    if True or send_ready_count > 1:
                      self._send_now.set()
                      send_ready_count = 0

                    # If a frame has been queued for transmit, send it.
                    if False and not self._send_queue.empty():
                        # 0.09 works best so far
                        #Timer(0.09 , self._send_frame).start()
                        #Timer(0.091 , self._send_frame).start()
                        #Thread(target=self._send_frame).start()
                        #self._send_frame()
                        if send_ready_count > 1:
                          self._send_now.set()
                          #Thread(target=self._send_frame).start()
                          Timer(0.0005 , self._send_frame).start()
                          #Timer(0.09 , self._send_frame).start()
                          send_ready_count = 0

                    send_ready_count += 1


                    continue
                elif frame_type == self.FRAME_TYPE_LOCAL_WIRED_KEY_EVENT:
                    self._kp_pending_events[frame_start_time] = ["wire",frame] + list(self._kp_display_trace)
                    if keep_alive_time:
                      _LOGGER.debug('%3.3f: Local Wired Key: %s since keep-alive: %3.5f src=%d',
                                    frame_start_time, binascii.hexlify(frame),
                                    frame_start_time - keep_alive_time,
                                    last_send_ready_count)
                elif frame_type == self.FRAME_TYPE_REMOTE_WIRED_KEY_EVENT:
                    self._kp_pending_events[frame_start_time] = ["remt", frame] + list(self._kp_display_trace)
                    _LOGGER.debug('%3.3f: Remote Wired Key: %s',
                                  frame_start_time, binascii.hexlify(frame))
                elif frame_type == self.FRAME_TYPE_WIRELESS_KEY_EVENT:
                    if frame != bytearray(b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00'):
                      self._kp_pending_events[frame_start_time] = ["wrls", frame] + list(self._kp_display_trace)
                    if keep_alive_time:
                      _LOGGER.debug('%3.3f: Wireless Key : %s since keep-alive: %3.5f src=%d',
                                    frame_start_time, binascii.hexlify(frame),
                                    frame_start_time - keep_alive_time,
                                    last_send_ready_count)
                elif frame_type == self.FRAME_TYPE_LEDS:
                    # _LOGGER.debug('%3.3f: LEDs: %s',
                    #              frame_start_time, binascii.hexlify(frame))
                    # First 4 bytes are the LEDs that are on;
                    # second 4 bytes_ are the LEDs that are flashing
                    states = int.from_bytes(frame[0:4], byteorder='little')
                    flashing_states = int.from_bytes(frame[4:8],
                                                     byteorder='little')
                    states |= flashing_states
                    if self._heater_auto_mode:
                        states |= States.HEATER_AUTO_MODE
                    if (states != self._states or
                            flashing_states != self._flashing_states):
                        self._states = states
                        self._flashing_states = flashing_states
                        threaded_cb()
                elif frame_type == self.FRAME_TYPE_PUMP_SPEED_REQUEST:
                    value = int.from_bytes(frame[0:2], byteorder='big')
                    _LOGGER.debug('%3.3f: Pump speed request: %d%%',
                                  frame_start_time, value)
                    if self._pump_speed != value:
                        self._pump_speed = value
                        threaded_cb()
                elif ((frame_type == self.FRAME_TYPE_PUMP_STATUS) and
                      (len(frame) >= 5)):
                    # Pump status messages sent out by Hayward VSP pumps
                    self._multi_speed_pump = True
                    speed = frame[2]
                    # Power is in BCD
                    power = ((((frame[3] & 0xf0) >> 4) * 1000) +
                             (((frame[3] & 0x0f)) * 100) +
                             (((frame[4] & 0xf0) >> 4) * 10) +
                             (((frame[4] & 0x0f))))
                    _LOGGER.debug('%3.3f; Pump speed: %d%%, power: %d watts',
                                  frame_start_time, speed, power)
                    if self._pump_power != power:
                        self._pump_power = power
                        threaded_cb()
                elif frame_type == self.FRAME_TYPE_DISPLAY_UPDATE:
                    self.kp_display_update(frame)

                    try:
                      display_frame = bytearray()
                      for v in frame:
                        if v == 0xdf or v == 0x5f:
                          # Convert LCD-specific degree symbol and decode to utf-8
                          display_frame.extend(b'\xc2\xb0')
                        else:
                          # Flashing values are encoded with bit 8 set
                          display_frame.append(0x7f & v)
                      text = display_frame.decode('utf-8')
                    except UnicodeDecodeError as e:
                      _LOGGER.info(f"decerr: {frame}: {e}")
                      continue

                    parts = text.split()
                    _LOGGER.debug('%3.3f: Display update: %s',
                                  frame_start_time, parts)

                    if display_callback:
                        Thread(target=display_callback, args=[text]).start()
                    if self._web:
                      self._web.text_updated(text)

                    try:
                        if (parts[0] == 'Pool' or parts[0] == 'Spa') and parts[1] == 'Temp':
                            # Pool Temp <temp>°[C|F]
                            value = int(parts[2][:-2])
                            if self._pool_temp != value:
                                self._pool_temp = value
                                self._is_metric = parts[2][-1:] == 'C'
                                threaded_cb()
                        elif parts[0] == 'Spa' and parts[1] == 'Temp':
                            # Spa Temp <temp>°[C|F]
                            value = int(parts[2][:-2])
                            if self._spa_temp != value:
                                self._spa_temp = value
                                self._is_metric = parts[2][-1:] == 'C'
                                threaded_cb()
                        elif parts[0] == 'Air' and parts[1] == 'Temp':
                            # Air Temp <temp>°[C|F]
                            value = int(parts[2][:-2])
                            if self._air_temp != value:
                                self._air_temp = value
                                self._is_metric = parts[2][-1:] == 'C'
                                threaded_cb()
                        elif parts[0] == 'Pool' and parts[1] == 'Chlorinator':
                            # Pool Chlorinator <value>%
                            value = int(parts[2][:-1])
                            if self._pool_chlorinator != value:
                                self._pool_chlorinator = value
                                threaded_cb()
                        elif parts[0] == 'Spa' and parts[1] == 'Chlorinator':
                            # Spa Chlorinator <value>%
                            value = int(parts[2][:-1])
                            if self._spa_chlorinator != value:
                                self._spa_chlorinator = value
                                threaded_cb()
                        elif parts[0] == 'Salt' and parts[1] == 'Level':
                            # Salt Level <value> [g/L|PPM|
                            value = float(parts[2])
                            if self._salt_level != value:
                                self._salt_level = value
                                self._is_metric = parts[3] == 'g/L'
                                threaded_cb()
                        elif parts[0] == 'Check' and parts[1] == 'System':
                            # Check System <msg>
                            value = ' '.join(parts[2:])
                            if self._check_system_msg != value:
                                self._check_system_msg = value
                                threaded_cb()
                        elif parts[0] == 'Heater1':
                            self._heater_auto_mode = parts[1] == 'Auto'
                    except ValueError:
                        pass
                elif frame_type == self.FRAME_TYPE_LONG_DISPLAY_UPDATE:
                    self.kp_display_update(frame)
                    if last_long_display != frame:
                      self._send_ack.set()
                    last_long_display = frame
                    _LOGGER.debug('%3.3f: long received', time.monotonic())
                    # Not currently parsed
                    pass
                else:
                    _LOGGER.debug('%3.3f: Unknown frame: %s %s',
                                 frame_start_time,
                                 binascii.hexlify(frame_type),
                                 binascii.hexlify(frame))
        except socket.timeout:
            _LOGGER.info("socket timeout")
        except serial.SerialTimeoutException:
            _LOGGER.info("serial timeout")
        except EOFError:
            _LOGGER.info("eof")

    def _append_data(self, frame, data):
        for byte in data:
            frame.append(byte)
            if byte == self.FRAME_DLE:
                frame.append(0)

    def _get_key_event_frame(self, key):
        frame = bytearray()
        frame.append(self.FRAME_DLE)
        frame.append(self.FRAME_STX)

        if False:
            self._append_data(frame, self.FRAME_TYPE_REMOTE_WIRED_KEY_EVENT)
            self._append_data(frame, key.value.to_bytes(4, byteorder='big'))
            self._append_data(frame, key.value.to_bytes(4, byteorder='big'))
        elif False:
            self._append_data(frame, self.FRAME_TYPE_LOCAL_WIRED_KEY_EVENT)
            self._append_data(frame, key.value.to_bytes(4, byteorder='big'))
            self._append_data(frame, key.value.to_bytes(4, byteorder='big'))
        elif True or key.value > 0xffff:
            self._append_data(frame, self.FRAME_TYPE_WIRELESS_KEY_EVENT)
            self._append_data(frame, b'\x01')
            self._append_data(frame, key.value.to_bytes(4, byteorder='big'))
            self._append_data(frame, key.value.to_bytes(4, byteorder='big'))
            self._append_data(frame, b'\x00')
        else:
            self._append_data(frame, self.FRAME_TYPE_LOCAL_WIRED_KEY_EVENT)
            self._append_data(frame, key.value.to_bytes(2, byteorder='little'))
            self._append_data(frame, key.value.to_bytes(2, byteorder='little'))

        crc = 0
        for byte in frame:
            crc += byte
        self._append_data(frame, crc.to_bytes(2, byteorder='big'))

        if False:
          # 0x10 is escaped
          frame = frame[0:2] + frame[2:].replace(
                           b'\x10', b'\x10\x00')
        if (len(frame) % 2) == 1:
              _LOGGER.warning(f'frame has odd bit count {frame}')

        frame.append(self.FRAME_DLE)
        frame.append(self.FRAME_ETX)

        return frame

    def send_key(self, key):
        """Sends a key."""
        _LOGGER.info('Queueing key %s', key)
        frame = self._get_key_event_frame(key)

        # Queue it to send immediately following the reception
        # of a keep-alive packet in an attempt to avoid bus collisions.
        self._send_queue.put({'frame': frame, 'desired_states': None})

    @property
    def air_temp(self):
        """Returns the current air temperature, or None if unknown."""
        return self._air_temp

    @property
    def pool_temp(self):
        """Returns the current pool temperature, or None if unknown."""
        return self._pool_temp

    @property
    def spa_temp(self):
        """Returns the current spa temperature, or None if unknown."""
        return self._spa_temp

    @property
    def pool_chlorinator(self):
        """Returns the current pool chlorinator level in %,
        or None if unknown."""
        return self._pool_chlorinator

    @property
    def spa_chlorinator(self):
        """Returns the current spa chlorinator level in %,
        or None if unknown."""
        return self._spa_chlorinator

    @property
    def salt_level(self):
        """Returns the current salt level, or None if unknown."""
        return self._salt_level

    @property
    def check_system_msg(self):
        """Returns the current 'Check System' message, or None if unknown."""
        if self.get_state(States.CHECK_SYSTEM):
            return self._check_system_msg
        return None

    @property
    def status(self):
        """Returns 'OK' or the current 'Check System' message."""
        if self.get_state(States.CHECK_SYSTEM):
            return self._check_system_msg
        return 'OK'

    @property
    def pump_speed(self):
        """Returns the current pump speed in percent, or None if unknown.
           Requires a Hayward VSP pump connected to the AquaLogic bus."""
        return self._pump_speed

    @property
    def pump_power(self):
        """Returns the current pump power in watts, or None if unknown.
           Requires a Hayward VSP pump connected to the AquaLogic bus."""
        return self._pump_power

    @property
    def is_metric(self):
        """Returns True if the temperature and salt level values
        are in Metric."""
        return self._is_metric

    @property
    def is_heater_enabled(self):
        """Returns True if HEATER_1 is on"""
        return self.get_state(States.HEATER_1)

    @property
    def is_super_chlorinate_enabled(self):
        """Returns True if super chlorinate is on"""
        return self.get_state(States.SUPER_CHLORINATE)

    def states(self):
        """Returns a set containing the enabled states."""
        state_list = []
        for state in States:
            if state.value & self._states != 0:
                state_list.append(state)

        if (self._flashing_states & States.FILTER) != 0:
            state_list.append(States.FILTER_LOW_SPEED)

        return state_list

    def get_state(self, state):
        """Returns True if the specified state is enabled."""
        # Check to see if we have a change request pending; if we do
        # return the value we expect it to change to.
        for data in list(self._send_queue.queue):
            desired_states = data['desired_states']
            if desired_states is None:
              continue
            for desired_state in desired_states:
                if desired_state['state'] == state:
                    return desired_state['enabled']
        if state == States.FILTER_LOW_SPEED:
            return (States.FILTER.value & self._flashing_states) != 0
        return (state.value & self._states) != 0

    def set_state(self, state, enable):
        """Set the state."""

        is_enabled = self.get_state(state)
        if is_enabled == enable:
            return True

        key = None

        if state == States.FILTER_LOW_SPEED:
            if not self._multi_speed_pump:
                return False
            # Send the FILTER key once.
            # If the pump is in high speed, it wil switch to low speed.
            # If the pump is off the retry mechanism will send an additional
            # FILTER key to switch into low speed.
            # If the pump is in low speed then we pretend the pump is off;
            # the retry mechanism will send an additional FILTER key
            # to switch into high speed.
            key = Keys.FILTER
            desired_states = [{'state': state, 'enabled': not is_enabled}]
            desired_states.append({'state': States.FILTER, 'enabled': True})
        elif state == States.HEATER_AUTO_MODE:
            key = Keys.HEATER_1
            # Flip the heater mode
            desired_states = [{'state': States.HEATER_AUTO_MODE,
                               'enabled': not self._heater_auto_mode}]
        elif state == States.POOL or state == States.SPA:
            key = Keys.POOL_SPA
            desired_states = [{'state': state, 'enabled': not is_enabled}]
        elif state == States.HEATER_1:
            # TODO: is there a way to force the heater on?
            # Perhaps press & hold?
            return False
        else:
            # See if this state has a corresponding Key
            try:
                key = Keys[state.name]
            except KeyError:
                # TODO: send the appropriate combination of keys
                # to enable the state
                return False
            desired_states = [{'state': state, 'enabled': not is_enabled}]

        frame = self._get_key_event_frame(key)

        # Queue it to send immediately following the reception
        # of a keep-alive packet in an attempt to avoid bus collisions.
        self._send_queue.put({'frame': frame, 'desired_states': desired_states,
                              'retries': 0})

        return True

    def enable_multi_speed_pump(self, enable):
        """Enables multi-speed pump mode."""
        self._multi_speed_pump = enable
        return True
