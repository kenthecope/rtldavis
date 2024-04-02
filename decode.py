#!/usr/bin/env /home/copek/work/src/github.com/kenthecope/rtldavis/ve/bin/python3
# from https://github-wiki-see.page/m/philcovington/rtldavis/wiki/Davis-Message-Format

"""
Byte 0: This is a header. The upper nibble is the sensor the data is from, as follows. Some of these messages are Vantage Vue specific.

2 = Supercap voltage (Vue only)
3 = ?
4 = UV Index
5 = Rain rate
6 = Solar radiation
7 = Solar Cell output (Vue only)
8 = Temperature
9 = Wind gust
a = Humidity
e = Rain
The lowest three bits in the low order nibble is the transmitter ID, set via dipswitches inside the unit.

"""

# 8 bits of data
"""
# Test data
raw_packet="90004F0103054610"
raw_packet="80014F1DAB0004B5"
raw_packet="A0004FAF3B001398"
raw_packet="E0004F320300CFDC"
raw_packet="500053FE73003B2A"
raw_packet="400009FFC3001503"
packet=bytearray.fromhex(raw_packet)
print (packet)
print (packet[0], hex(packet[0]))
print (packet[1])
print (packet[2])
print (packet[3])
print (packet[4])
print (packet[5])
print (packet[6])
print (packet[7])
"""
import subprocess
import os
import paho.mqtt.client as mqtt
from socket import gaierror
import json
import datetime

config = {'homeassistant':True,
          'mqtt_server':"10.0.18.83",
          'mqtt_port': 1883,
          'mqtt_username':'rtldavis',
          'mqtt_password':'RTLd@v1s_'
         }

crc_counter = 0   # tally of CRC errors
rain_since_midnight = 0  # counter of bucket tips, rollover at 127

# sensors on board for the Vanatage Pro 2
sensors =  { 
    "temperature": {
                    "dev_cla":"temperature",
                    "name": "Outside Temperature",
                    "unit_of_meas": "°C",
                    "stat_cla": 'measurement',
                    "stat_t": "rtldavis/sensor/temperature/state",
                    "avty_t": "rtldavis/status",
                    "uniq_id": "VantagePro2_Outside_Temperature",
                    "dev": {
                            "ids":"rtldavis",
                            "name": "RTLDavis Weather Station",
                            "model": "Vantage Pro 2",
                            "mf": "Davis"
                           }
                    },
    "wind_speed": {
                    "dev_cla":"wind_speed",
                    "name": "Wind Speed",
                    "unit_of_meas": "Km/h",
                    "stat_cla": 'measurement',
                    "stat_t": "rtldavis/sensor/wind_speed/state",
                    "avty_t": "rtldavis/status",
                    "uniq_id": "VantagePro2_Wind_Speed",
                    "dev": {
                            "ids":"rtldavis",
                            "name": "RTLDavis Weather Station",
                            "model": "Vantage Pro 2",
                            "mf": "Davis"
                           }
                    },
    "wind_gust": {
                    "dev_cla":"wind_speed",
                    "name": "Wind Gust",
                    "unit_of_meas": "Km/h",
                    "stat_cla": 'measurement',
                    "stat_t": "rtldavis/sensor/wind_gust/state",
                    "avty_t": "rtldavis/status",
                    "uniq_id": "VantagePro2_Wind_Gust",
                    "dev": {
                            "ids":"rtldavis",
                            "name": "RTLDavis Weather Station",
                            "model": "Vantage Pro 2",
                            "mf": "Davis"
                           }
                    },
    "rain_counter": {
                    "name": "Rain Counter",
                    "stat_cla": 'measurement',
                    "stat_t": "rtldavis/sensor/rain_counter/state",
                    "avty_t": "rtldavis/status",
                    "uniq_id": "VantagePro2_rain_counter",
                    "dev": {
                            "ids":"rtldavis",
                            "name": "RTLDavis Weather Station",
                            "model": "Vantage Pro 2",
                            "mf": "Davis"
                           }
                    },
    "rain_since_midnight": {
                    "dev_cla":"distance",
                    "name": "Rain",
                    "unit_of_meas": "mm",
                    "stat_cla": 'measurement',
                    "stat_t": "rtldavis/sensor/rain_since_midnight/state",
                    "avty_t": "rtldavis/status",
                    "uniq_id": "VantagePro2_Daily_Rain",
                    "dev": {
                            "ids":"rtldavis",
                            "name": "RTLDavis Weather Station",
                            "model": "Vantage Pro 2",
                            "mf": "Davis"
                           }
                    },
    "rain_rate": {
                    "name": "Rain Rate",
                    "stat_cla": 'measurement',
                    "unit_of_meas": "mm/h",
                    "stat_t": "rtldavis/sensor/rain_rate/state",
                    "avty_t": "rtldavis/status",
                    "uniq_id": "VantagePro2_Rain_Rate",
                    "dev": {
                            "ids":"rtldavis",
                            "name": "RTLDavis Weather Station",
                            "model": "Vantage Pro 2",
                            "mf": "Davis"
                           }
                    },
    "wind_dir": {
                    "name": "Wind Direction",
                    "unit_of_meas": "°",
                    "stat_cla": 'measurement',
                    "stat_t": "rtldavis/sensor/wind_dir/state",
                    "avty_t": "rtldavis/status",
                    "uniq_id": "VantagePro2_Wind_Direction",
                    "dev": {
                            "ids":"rtldavis",
                            "name": "RTLDavis Weather Station",
                            "model": "Vantage Pro 2",
                            "mf": "Davis"
                           }
                    },
    "humidity": {
                    "dev_cla":"humidity",
                    "name": "Outside Relative Humidity",
                    "unit_of_meas": "%",
                    "stat_cla": 'measurement',
                    "stat_t": "rtldavis/sensor/humidity/state",
                    "avty_t": "rtldavis/status",
                    "uniq_id": "VantagePro2_Outside_Humidity",
                    "dev": {
                            "ids":"rtldavis",
                            "name": "RTLDavis Weather Station",
                            "model": "Vantage Pro 2",
                            "mf": "Davis"
                           }
                    }
}


def since_midnight():
    # for new day detection, depends on time of host system being in correct TZ
    now = datetime.datetime.now()
    return (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()



class MQTTmessage(object):
    def __init__(self, main_topic='rtldavis', topic=None, sub_topic=None, msg = "", homeassistant=False):
        self.main_topic = main_topic
        self.topic = topic
        self.sub_topic = sub_topic
        self.msg = msg
        self.homeassistant = homeassistant

    @property
    def message(self):
        # return a tuple for MQTT, or a None if no message is ready
        topic = self.main_topic
        if self.topic:
            if topic[-1] != '/':
                topic += f'/'
            topic += f'{self.topic}'
        if self.sub_topic:
            if topic[-1] != '/':
                topic += f'/'
            topic += f'{self.sub_topic.replace(" ", "_")}'
        if self.msg:
            return (topic, self.msg)
        return None




class HomeAssitantIntegrator(object):
    """
    Create integration messages for Home Assistant auto discovery based on the sensors config

    The discovery topic needs to follow a specific format:

    <discovery_prefix>/<component>/[<node_id>/]<object_id>/config
    Text
    <discovery_prefix>: The Discovery Prefix defaults to homeassistant. This prefix can be changed.
    <component>: One of the supported MQTT integrations, eg. binary_sensor.
    <node_id> (Optional): ID of the node providing the topic, this is not used by Home Assistant but may be used to structure the MQTT topic. The ID of the node must only consist of characters from the character class [a-zA-Z0-9_-] (alphanumerics, underscore and hyphen).
    <object_id>: The ID of the device. This is only to allow for separate topics for each device and is not used for the entity_id. The ID of the device must only consist of characters from the character class [a-zA-Z0-9_-] (alphanumerics, underscore and hyphen).
    The <node_id> level can be used by clients to only subscribe to their own (command) topics by using one wildcard topic like <discovery_prefix>/+/<node_id>/+/set.

    Best practice for entities with a unique_id is to set <object_id> to unique_id and omit the <node_id>.

    """
    def __init__(self, sensors, client=None):
        self.client = client
        self.discovery_prefix = 'homeassistant'
        self.messages = []
        self.sensors = sensors


    def publish(self):
        print (self.sensors)
        for sensor, config in self.sensors.items():
            print ("SENSOR:", sensor)
            topic = f"{self.discovery_prefix}/sensor/rtldavis/{sensor}/config"
            print ("TOPIC:", topic)
            self.client.publish(topic, json.dumps(config), 0 , True)



def reflect_data(x, width):
    # See: https://stackoverflow.com/a/20918545
    if width == 8:
        x = ((x & 0x55) << 1) | ((x & 0xAA) >> 1)
        x = ((x & 0x33) << 2) | ((x & 0xCC) >> 2)
        x = ((x & 0x0F) << 4) | ((x & 0xF0) >> 4)
    elif width == 16:
        x = ((x & 0x5555) << 1) | ((x & 0xAAAA) >> 1)
        x = ((x & 0x3333) << 2) | ((x & 0xCCCC) >> 2)
        x = ((x & 0x0F0F) << 4) | ((x & 0xF0F0) >> 4)
        x = ((x & 0x00FF) << 8) | ((x & 0xFF00) >> 8)
    elif width == 32:
        x = ((x & 0x55555555) << 1) | ((x & 0xAAAAAAAA) >> 1)
        x = ((x & 0x33333333) << 2) | ((x & 0xCCCCCCCC) >> 2)
        x = ((x & 0x0F0F0F0F) << 4) | ((x & 0xF0F0F0F0) >> 4)
        x = ((x & 0x00FF00FF) << 8) | ((x & 0xFF00FF00) >> 8)
        x = ((x & 0x0000FFFF) << 16) | ((x & 0xFFFF0000) >> 16)
    else:
        raise ValueError('Unsupported width')
    return x

def crc_poly(data, n, poly, crc=0, ref_in=False, ref_out=False, xor_out=0):
    g = 1 << n | poly  # Generator polynomial

    # Loop over the data
    for d in data:
        # Reverse the input byte if the flag is true
        if ref_in:
            d = reflect_data(d, 8)

        # XOR the top byte in the CRC with the input byte
        crc ^= d << (n - 8)

        # Loop over all the bits in the byte
        for _ in range(8):
            # Start by shifting the CRC, so we can check for the top bit
            crc <<= 1

            # XOR the CRC if the top bit is 1
            if crc & (1 << n):
                crc ^= g

    # Reverse the output if the flag is true
    if ref_out:
        crc = reflect_data(crc, n)

    # Return the CRC value
    return crc ^ xor_out


def process_data(raw_packet):
    print ("\n\n-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
    packet=bytearray.fromhex(raw_packet)
    # CRC-16/XMODEM
    crc = crc_poly(bytes(packet[:6]), 16, 0x1021)
    print(hex(crc), '{0:016b}'.format(crc), crc)
    packet_crc = packet[6] * 256 + packet[7]
    print (packet_crc)
    if packet_crc == crc:
        print ("CRC CHECKS OUT")
    else:
        print ("THIS PACKET IS SHIT!")
        crc_counter += 1
        return {'crc':crc_counter }

    # dict of the weather data
    weather = { }

    # protocol breakdown from 
    # analyze byte 0 
    print (raw_packet)
    # print ("Byte 0", packet[0], hex(packet[0]), bin(packet[0]))
    # shift byte 0 right by 4 bitsi
    message_type = packet[0] >> 4
    print ("MESSAGE TYPE:", message_type)
    if message_type == 5:
        # Bytes 3 and 4 contain the rain rate information. The rate is actually the time in seconds between rain bucket tips in the ISS.
        # The rain rate is calculated from the bucket tip rate and the size of the bucket (0.01" of rain for units sold in North America)
        if packet[3] == 0xff:
            # no rain
            rain_rate = None
        # light rain
        elif (packet[4]) & 0x40 == 0:
            rain_rate = 0
        elif (packet[4]) & 0x40 == 0x40:
            rain_rate = 0
        # light rain??? mm/h
        else:
            bucket_dump = ((packet[4] & 0x30) / 16 * 250) + packet[3]
            rain_rate =  720 / (((packet[4] & 0x30) / 16 * 250) + packet[3])
        print ("RAIN_RATE:", rain_rate, "mm/h")
        weather['rain_rate'] = rain_rate
    elif message_type == 8:
        # Byte 3 and 4 are temperature. The first byte is MSB and the second LSB. The value is signed with 0x0000 representing 0F.
        tempF = ((packet[3] * 256 + packet[4])) / 160
        tempC = (tempF - 32)/ 1.79999999
        print ("TEMPERATURE", tempC, "C")
        weather['temperature'] = round(tempC, 1)
    elif message_type == 4:
        print ("UV INDEX")
    elif message_type == 9:
        # This message transmits the maximum wind speed during the last 10 minutes (it appears to be the maximum in the last ten message
        # 9 intervals, for an exact time of packet_period * message_9_period * 10 = (40 + id) / 16 * 20 * 10). The units are in miles per hour
        # The upper nibble of byte 5 contains an index ranging from 0 to 9 that indicates which of the last ten message 9
        # intervals the gust occurred in. 
        gust = int(packet[3]) * 1.609344
        gust_idx = int(packet[5] >> 4)
        weather['wind_gust'] = round(gust, 1)
        weather['wind_gust_idx'] = gust_idx
        print ("WIND GUST", gust, 'km/h', gust_idx)
    elif message_type == 10:
        #Humidity is represented as two bytes in Byte 3 and Byte 4 as a ten bit value.
        humidity = (((packet[4] >> 4) << 8) + packet[3]) / 10.0
        weather['humidity'] = round(humidity, 1)
        print ("HUMIDITY", humidity, "%")
    elif message_type == 14:
        print ("RAIN")
        # It is a running total of bucket tips that wraps back around to 0 eventually from the ISS.
        # It is up to the console to keep track of changes in this byte. Only bits 0 through 6 of byte 3 are used,
        # so the counter will overflow after 0x7F (127).
        print ("RAIN", int(packet[3]), "bucket tip counter" )
        weather['rain_counter'] = int(packet[3])
    else:
        print ("NOT DEFINED")

    # Byte 1 - wind speed in mph
    wind_speed = int(packet[1]) * 1.609344
    # Byte 2 wind dir from 1 to 360 degrees
    # Wind direction from 1 to 360 degrees. Wind direction is updated every transmission. 
    # The wind reading is contained in a single byte that limits the maximum value to 255.
    wind_dir = int(packet[2])
    if wind_dir == 0:
        wind_dir == 360
    else:
        wind_dir = 9 + wind_dir * 342.0 / 255.0;

    print ("WIND SPEED:", wind_speed)
    print ("WIND DIR:", wind_dir)
    weather['wind_speed'] = round(wind_speed, 1)
    weather['wind_dir'] = round(wind_dir, 0)
    print ("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

    return weather


def parse_packet(line):
    # find a valid data byte
    words = line.split()
    result = None
    if len(words) > 3:
        if len(words[1]) == 16:
            result = process_data(words[1])
        else:
            print ("LINE:", line)
    if result:
        return result


def on_connect(client, userdata, flags, rc, properties):
    print(f"MQTT Connected to {client._host} on port {client._port} with result code {rc} ")
    # birth message
    if rc == 0:
        client.publish("rtldavis/status", "online", 0, True)
    elif rc == 1:
        print ("Connection refused.")
    elif rc == 2:
        print ("Invalid client id")
    elif rc == 3:
        print ("Server unavailale")
    elif rc == 4:
        print ("Bad authentication")
    elif rc == 5:
        print ("Not authorised")
    else:
        print ("Bad connection!")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("MQTT Unexpected disconnection.")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))


def main():

    seconds_since_midnight = since_midnight()
    rain_counter = None
    rain_since_midnight = 0


    try:
        client.connect(config['mqtt_server'], mqtt_port, 60)
    except ConnectionRefusedError as err:
        print (f"Connection Refused to {config['mqtt_server']} on port {config['mqtt_port']} - will auto reconnect")
    except gaierror:
        print (f"Cannot resolve host {config['mqtt_server']}")
        sys.exit(1)

    # HA autodicovery
    # print (config)
    if 'homeassistant' in config and config['homeassistant']:
        print ("Home Assistant Auto Discovery Enabled")
        ha = HomeAssitantIntegrator(sensors, client=client)
        ha.publish()

    client.loop_start()

    with subprocess.Popen(['/home/copek/work/bin/rtldavis'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
        for line in process.stdout:
            result = parse_packet(line.decode('utf8').strip())
            if result:
                if since_midnight() < seconds_since_midnight:
                    # new day
                    rain_since_midnight = 0
                else:
                    # update old clock
                    seconds_since_midnight = since_midnight()

                # send rain if rain counter in weather dict
                if 'rain_counter' in result:
                    # initialize the rain counter if needed for the day
                    if rain_counter is None:
                        rain_counter = result['rain_counter']   # current reading
                        rain_counter_prev = result['rain_counter']  # last known reading
                    else:
                        if result['rain_counter'] != rain_counter_prev:
                            # look for 7-bit counter rollover 
                            if rain_counter_prev > result['rain_counter']:
                                # rollover occured
                                seconds_since_midnight += 127 - rain_counter_prev + result['rain_counter']
                            else:
                                seconds_since_midnight += result['rain_counter']  - rain_counter_prev
                    result['rain_since_midnight'] = rain_since_midnight * 0.254   # 0.01 inch per bucket tip
                weather_json = json.dumps(result)
                # print ("RESULT:", result, weather_json)
                for subtopic, status in result.items():
                    client.publish(f'rtldavis/sensor/{subtopic}/state', json.dumps(status), qos=2, retain=True)

if __name__ == '__main__':
    print ("RTLDavis to MQTT Gateway")

    client_id = "rtldavis-mqtt-gw"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=True )
    # set the last will message that we are now unavailable
    client.will_set("rtldavis/status", "offline", 0, False)


        # input checking
    if 'mqtt_server' not in config:
        print ("ERROR: no mqtt_server defined!")
        sys.exit(1)

    def report_port_error(port):
        print ("mqtt_port {port} in {args.config_file} is not an integer between 1 and 65535!")
        sys.exit(1)

    if 'mqtt_port' not in config:
        mqtt_port = 1883
    elif not isinstance(config['mqtt_port'], int):
        report_port_error(config['mqtt_port'])
    elif config['mqtt_port'] < 1 or config['mqtt_port'] > 65535:
        report_port_error(config['mqtt_port'])
    else:
        mqtt_port = config['mqtt_port']


    if config['mqtt_username'] and config['mqtt_password']:
        client.username_pw_set(username=config['mqtt_username'], password=config['mqtt_password'])
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message


    main()
