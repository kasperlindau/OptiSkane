import json
import requests
from io import BytesIO
from zipfile import ZipFile
from google.protobuf import json_format
from google.transit import gtfs_realtime_pb2


class TrafikLab(object):
    def __init__(self, OPERATOR, STATIC_API_KEY, REALTIME_API_KEY):
        # Endpoints
        self.STATIC_DATA_URL = f"https://opendata.samtrafiken.se/gtfs/{OPERATOR}/{OPERATOR}.zip?key={STATIC_API_KEY}"
        self.SERVICE_ALERTS_URL = f"https://opendata.samtrafiken.se/gtfs-rt/{OPERATOR}/ServiceAlerts.pb?key={REALTIME_API_KEY}"
        self.TRIP_UPDATES_URL = f"https://opendata.samtrafiken.se/gtfs-rt/{OPERATOR}/TripUpdates.pb?key={REALTIME_API_KEY}"
        self.VEHICLE_POSITIONS_URL = f"https://opendata.samtrafiken.se/gtfs-rt/{OPERATOR}/VehiclePositions.pb?key={REALTIME_API_KEY}"

    def download_static_data(self):
        r = requests.get(self.STATIC_DATA_URL)
        zf = ZipFile(BytesIO(r.content))
        zf.extractall("data")

    def get_service_alerts(self):
        r = requests.get(self.SERVICE_ALERTS_URL)
        return self._pb_to_json(r.content)

    def get_trip_updates(self):
        r = requests.get(self.TRIP_UPDATES_URL)
        return self._pb_to_json(r.content)

    def get_vehicle_positions(self):
        r = requests.get(self.VEHICLE_POSITIONS_URL)
        return self._pb_to_json(r.content)

    def _pb_to_json(self, pb_bytes):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(pb_bytes)
        feed_json = json.loads(json_format.MessageToJson(feed))
        return feed_json
