import os
import time
import logging
import pandas as pd
import datetime as dt
from src.utils import *
from threading import Timer
from src.trafiklab import TrafikLab
from concurrent.futures import ThreadPoolExecutor


class OptiSkane:
    def __init__(self, WALK_SPEED=5, MAX_WALK_RADIUS=1, MAX_RAPTOR_ROUNDS=3):
        # Options
        self.WALK_SPEED = WALK_SPEED
        self.MAX_WALK_RADIUS = MAX_WALK_RADIUS
        self.MAX_RAPTOR_ROUNDS = MAX_RAPTOR_ROUNDS  # max_n_transfers

        # TrafikLab client setup
        self.trafiklab = TrafikLab(
            OPERATOR="skane",
            STATIC_API_KEY=os.environ["TRAFIKLAB_STATIC_KEY"],
            REALTIME_API_KEY=os.environ["TRAFIKLAB_REALTIME_KEY"]
        )
        self.scheduled_sleeps = None  # sets each static-update (every 24h)
        self.REQUESTS_TABLE = {  # realtime updates per hour
            0: 12,
            1: 12,
            2: 12,
            3: 0,
            4: 0,
            5: 0,
            6: 12,
            7: 48,
            8: 48,
            9: 48,
            10: 24,
            11: 24,
            12: 48,
            13: 48,
            14: 48,
            15: 48,
            16: 48,
            17: 48,
            18: 48,
            19: 24,
            20: 24,
            21: 24,
            22: 24,
            23: 24
        }

        # RAPTOR requests pool (works as queue)
        self.process_pool = ThreadPoolExecutor(max_workers=1)
        # TODO: change to ProcessPoolExecutor?

        # Static data
        self.stops_df = None
        self.trips_df = None
        self.routes_df = None
        self.transfers_df = None
        self.stop_times_df = None
        self.calendar_dates_df = None

        # System data
        self.transits_dct = None

        self.stop_to_routes_map = None  # stop_id: [route_ids]
        self.route_to_stops_map = None  # rid: [stop_ids]
        self.route_stop_to_stop_sq_map = None  # (rid, stop_id): stop_sq

        self.route_stop_sq_to_dep_times_map = None  # (rid, stop_sq): [dep_times]
        self.route_stop_sq_to_trips_map = None  # (rid, stop_sq): [trip_ids]

        self.trip_to_arr_times_map = None  # trip_id: [arr_times]
        self.trip_to_dep_times_map = None  # trip_id: [dep_times]

        self.stop_to_trips_map = None  # stop_id: [trip_ids]
        self.stop_to_dep_times_map = None  # stop_id: [dep_times]

        # Used for realtime updates mappings
        self.trip_to_route_map = None  # trip_id: rid

        # Used for journey retrieval
        self.stop_to_stop_name_map = None  # stop_id: stop_name
        self.stop_to_platform_code_map = None  # stop_id: platform_code
        self.trip_stop_to_dep_time_map = None  # (trip_id, stop_id): dep_time
        self.trip_to_route_name_map = None  # trip_id: route_name

        # Prepare system
        self._refresh_static_system()
        self._update_realtime_data()

    def queue(self, request):
        """Queues a search request and returns the result."""
        logging.debug(request)
        future = self.process_pool.submit(self._search, request)
        return future.result()

    def _search(self, request):
        """Performs a search request."""

        # Getting starting & ending-stops
        starting_stops = self._get_walk_reachable_stops(request.origin)
        ending_stops = self._get_walk_reachable_stops(request.destination)
        logging.debug(f"Gathered {len(starting_stops)} starting stops & {len(ending_stops)} ending stops.")

        # Getting departures
        starting_stops, departure_times = self._get_departure_times(starting_stops, request.departure_time)
        logging.debug(f"Gathered {len(departure_times)} departure times.")

        # Finding earliest arrivals
        labels_lst = self._raptor(starting_stops, departure_times)
        logging.debug("Finding earliest arrivals done.")

        # Getting journeys
        journeys = self._get_journeys(labels_lst, starting_stops, ending_stops)
        logging.debug(f"{len(journeys)} journeys gathered.")

        # Filtering journeys
        filtered_journeys = self._filter_journeys(journeys)
        logging.debug(f"Filtered journeys: {len(filtered_journeys)}")

        return filtered_journeys

    def _get_walk_reachable_stops(self, coords):
        """Returns walk-reachable stops from coords sorted by time."""
        distance = haversine(coords, self.stops_df[["stop_lat", "stop_lon"]].values)
        stops = np.column_stack((self.stops_df["stop_id"].values, distance))
        stops = stops[distance < self.MAX_WALK_RADIUS]
        stops[:, 1] /= self.WALK_SPEED
        stops[:, 1] *= 3600 * 2  # penalizing
        return stops[stops[:, 1].argsort()]

    def _get_departure_times(self, starting_stops, departure_time):
        """Returns trip departure times for starting stops up to n_hours."""

        # Setting departure time
        if departure_time is None:
            departure_time = str(dt.datetime.now().time())[:8]
            logging.warning("Departure time is None. Setting it to current time.")
        departure_time = str_to_seconds(departure_time)

        # Filtering connected starting stops
        routes_dct = dict()
        for stop_id, walk_time in starting_stops:
            for r in self.stop_to_routes_map[stop_id]:
                if r not in routes_dct or walk_time < routes_dct[r][1]:
                    routes_dct[r] = [stop_id, walk_time]
        starting_stops = starting_stops[np.isin(starting_stops[:, 0], [lst[0] for lst in routes_dct.values()])]

        # Getting departures
        trips_dct = dict()
        for stop_id, walk_time in starting_stops:
            offset = np.searchsorted(self.stop_to_dep_times_map[stop_id], departure_time + walk_time)
            trips, deps = self.stop_to_trips_map[stop_id][offset:], self.stop_to_dep_times_map[stop_id][offset:]
            for x in range(len(trips)):
                if deps[x] > departure_time + walk_time + 3600 * 1:
                    break
                if trips[x] in trips_dct:
                    continue
                trips_dct[trips[x]] = deps[x] - walk_time
        all_departures = sorted(trips_dct.values())

        # Filter departure times by a interval
        departure_times = [all_departures[0]]
        for t in all_departures:
            if t - departure_times[-1] > 600:  # 10min
                departure_times.append(t)
        return starting_stops, departure_times

    def _raptor(self, starting_stops, departure_times):
        """Runs Microsoft's RAPTOR routing algorithm."""

        # Running RAPTOR algorithm for different departure-times
        t0 = time.time()
        labels_lst = list()
        for departure_time in departure_times:
            # Initialization
            t1 = time.time()
            star_label = dict()
            label = {k: dict() for k in range(self.MAX_RAPTOR_ROUNDS + 1)}
            marked_stops = list()

            # Initializing starting-stops
            for stop_id, walk_time in starting_stops:
                star_label[stop_id] = departure_time + walk_time
                label[0][stop_id] = (departure_time + walk_time, None, None)
                marked_stops.append(stop_id)

            # Running rounds
            for k in range(1, self.MAX_RAPTOR_ROUNDS + 1):
                # Accumulating routes for marked_stops
                Q = dict()
                for p in marked_stops:
                    routes_serving_p = self.stop_to_routes_map[p]
                    for r in routes_serving_p:
                        stop_idx = self.route_stop_sq_map[r, p]
                        if r not in Q or stop_idx < Q[r]:
                            Q[r] = stop_idx

                # Traverse each route
                trip_marked = list()
                for r, stop_idx in Q.items():
                    t = None
                    t_arrs = None
                    t_deps = None
                    boarding_stop = None

                    for pi in self.route_to_stops_map[r][stop_idx - 1:]:
                        # Checking if arrival time at stop with trip t is an improvement
                        if t is not None:
                            new_arrival = t_arrs[stop_idx - 1]
                            if pi not in star_label or new_arrival < star_label[pi]:
                                star_label[pi] = new_arrival
                                label[k][pi] = (new_arrival, t, boarding_stop)
                                trip_marked.append(pi)

                        # Checking if we can catch an earlier trip
                        if pi in label[k - 1] and (t is None or label[k - 1][pi][0] <= t_deps[stop_idx - 1]):
                            idx = np.searchsorted(self.route_stop_sq_to_dep_times_map[r, stop_idx], label[k - 1][pi][0])
                            trips = self.route_stop_sq_to_trips_map[r, stop_idx][idx:]
                            if len(trips) > 0:
                                t = trips[0]
                                t_arrs = self.trip_to_arr_times_map[t]
                                t_deps = self.trip_to_dep_times_map[t]
                                boarding_stop = pi
                            else:
                                t = None
                        stop_idx += 1

                # Foot-path reachable stops
                foot_marked = list()
                for p in trip_marked:
                    if p not in self.transits_dct:
                        continue
                    for pi, walk_time in self.transits_dct[p].items():
                        new_arrival = label[k][p][0] + walk_time
                        if pi not in star_label or new_arrival < star_label[pi]:
                            star_label[pi] = new_arrival
                            label[k][pi] = (new_arrival, "walking", p)
                            foot_marked.append(pi)
                marked_stops = set(trip_marked + foot_marked)

            labels_lst.append(label)
            logging.debug(f"Single RAPTOR round duration: {time.time() - t1}")
        logging.debug(f"All {len(departure_times)} RAPTOR rounds duration: {time.time() - t0}")
        return labels_lst

    def _get_journeys(self, labels_lst, starting_stops, ending_stops):
        """Returns best journeys for provided ending stops."""
        journeys = list()
        start_walk_time_dct = dict(zip(starting_stops[:, 0], starting_stops[:, 1]))
        for label in labels_lst:
            for end_stop_id, end_walk_time in ending_stops:
                for k in label:
                    if end_stop_id in label[k]:
                        # Getting path
                        path = list()
                        running_k = k
                        to_stop_id = end_stop_id
                        while running_k > 0:
                            # Helpers
                            cur_row = label[running_k][to_stop_id]
                            from_stop_id = cur_row[2]

                            # Getting sub-path info
                            if cur_row[1] == "walking":
                                route_name = "walking"
                                dep_time = cur_row[0] - end_walk_time
                            else:
                                route_name = self.trip_to_route_name_map[cur_row[1]]
                                dep_time = self.trip_stop_to_dep_time_map[cur_row[1], from_stop_id]
                            path.append({
                                "from_stop_name": self.stop_to_stop_name_map[from_stop_id],
                                "from_platform_code": self.stop_to_platform_code_map[from_stop_id],
                                "departure_time": dep_time,
                                "to_stop_name": self.stop_to_stop_name_map[to_stop_id],
                                "to_platform_code": self.stop_to_platform_code_map[to_stop_id],
                                "arrival_time": cur_row[0],
                                "route_name": route_name
                            })

                            # Update iteration
                            to_stop_id = cur_row[2]
                            if cur_row[1] != "walking":
                                running_k -= 1

                        # Adding start & end walks
                        if len(path) == 0:
                            continue
                        path.reverse()
                        path.insert(0, {
                            "from_stop_name": "origin",
                            "from_platform_code": None,
                            "departure_time": path[0]["departure_time"] - start_walk_time_dct[to_stop_id],
                            "to_stop_name": path[0]["from_stop_name"],
                            "to_platform_code": path[0]["from_platform_code"],
                            "arrival_time": path[0]["departure_time"],
                            "route_name": "walking"
                        })
                        path.append({
                            "from_stop_name": path[-1]["to_stop_name"],
                            "from_platform_code": path[-1]["to_platform_code"],
                            "departure_time": path[-1]["arrival_time"],
                            "to_stop_name": "destination",
                            "to_platform_code": None,
                            "arrival_time": path[-1]["arrival_time"] + end_walk_time,
                            "route_name": "walking"
                        })
                        journeys.append({
                            "path": path,
                            "n_transfers": k - 1,
                            "departure_time": path[0]["departure_time"],
                            "arrival_time": path[-1]["arrival_time"],
                            "total_duration": path[-1]["arrival_time"] - path[0]["departure_time"],
                        })
        return journeys

    def _filter_journeys(self, journeys):
        """Filters journeys based on criteria."""

        # Filter similar journeys by departure & arrival time
        filtered_journeys = dict()
        for journey in journeys:
            if journey["departure_time"] not in filtered_journeys:
                filtered_journeys[journey["departure_time"]] = journey
            if journey["arrival_time"] < filtered_journeys[journey["departure_time"]]["arrival_time"]:
                filtered_journeys[journey["departure_time"]] = journey
        return [filtered_journeys[k] for k in sorted(filtered_journeys.keys())]

    def _update_realtime_data(self):
        """Updates realtime data from Trafiklab."""
        # servicealerts are updated every 15s
        # tripupdates are updated every 15s
        # vehiclepositions are updated every 2s
        # max 30 000 calls per month (bronze)

        # Updating data
        r = self.trafiklab.get_trip_updates()
        t0 = int(dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        for t_update in r["entity"]:
            trip_id = t_update["tripUpdate"]["trip"]["tripId"]
            for s_update in t_update["tripUpdate"]["stopTimeUpdate"]:
                # Extracting info
                stop_id = s_update["stopId"]
                stop_sq = s_update["stopSequence"]
                new_arrival = int(s_update["arrival"]["time"]) - t0
                new_departure = int(s_update["departure"]["time"]) - t0

                # Updating data
                self.route_stop_sq_to_dep_times_map[self.trip_to_route_map[trip_id]][stop_sq] = new_departure
                self.trip_to_arr_times_map[trip_id][stop_sq - 1] = new_arrival
                self.trip_to_dep_times_map[trip_id][stop_sq - 1] = new_departure
                self.stop_to_dep_times_map[stop_id][self.stop_to_trips_map[stop_id].index(trip_id)] = new_departure

        # Schedule new update of realtime data
        logging.debug("Updated realtime data.")
        Timer(function=self._update_realtime_data, interval=self.scheduled_sleeps.pop(0)).start()

    def _refresh_static_system(self):
        # Updating static data
        self.trafiklab.download_static_data()
        logging.debug("New static data downloaded.")

        # Loading updated static data
        self._load_static_data()
        logging.debug("Loaded newly downloaded static data.")

        # Filtering stop-times-df for today's active trips
        self._filter_active_trips()
        logging.debug("Filtered today's active trips.")

        # Discovering possible transits
        self._discover_possible_transits()
        logging.debug("Discovered possible transits.")

        # Forming necessary data-mappings
        self._generate_data_mappings()
        logging.debug("Necessary data-mappings formed.")

        # Scheduling new realtime data updates
        self._schedule_realtime_data_updates()
        logging.debug(f"Scheduled {len(self.scheduled_sleeps)} realtime updates for the next 24h.")

        # Scheduling new system refresh 00:00:00
        midnight = (dt.datetime.now() + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        sleep = (midnight - dt.datetime.now()).total_seconds()
        Timer(function=self._refresh_static_system, interval=sleep).start()
        logging.info("System refreshed. Scheduled new refresh 24h from now.")

    def _schedule_realtime_data_updates(self):
        """Schedules realtime data updates every 24h."""
        t0 = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        timestamps = list()
        for k, v in self.REQUESTS_TABLE.items():
            timestamps.extend(t0 + 3600 * k + np.arange(v) * 3600 / v)
        self.scheduled_sleeps = np.diff(timestamps)

    def _generate_data_mappings(self):
        """Generates necessary data-mappings."""

        # Forming new routes (trips with same stop-sequence)
        self.stop_times_df.sort_values("dep_time", inplace=True)
        trip_stop_seqs = self.stop_times_df.groupby("trip_id")["stop_id"].apply(lambda lst: "-".join(lst))
        new_rids_df = trip_stop_seqs.astype("category").cat.codes.reset_index().rename(columns={0: "rid"})
        self.stop_times_df = self.stop_times_df.merge(new_rids_df, on="trip_id")

        # Forming route-names
        self.routes_df["route_name"] = self.routes_df["route_desc"] + " " + self.routes_df["route_short_name"].astype(str)
        self.stop_times_df = self.stop_times_df.merge(self.routes_df[["route_id", "route_name"]], on="route_id")

        # Mappings
        self.stop_times_df.sort_values("dep_time", inplace=True)
        self.stop_to_routes_map = self.stop_times_df.groupby("stop_id")["rid"].unique().to_dict()
        self.route_to_stops_map = self.stop_times_df.groupby("rid")["stop_id"].unique().to_dict()
        self.route_stop_sq_map = self.stop_times_df.groupby(["rid", "stop_id"])["stop_sequence"].first().to_dict()

        self.route_stop_sq_to_dep_times_map = self.stop_times_df.groupby(["rid", "stop_sequence"])["dep_time"].apply(list).to_dict()
        self.route_stop_sq_to_trips_map = self.stop_times_df.groupby(["rid", "stop_sequence"])["trip_id"].apply(list).to_dict()

        self.trip_to_arr_times_map = self.stop_times_df.groupby("trip_id")["arr_time"].apply(list).to_dict()
        self.trip_to_dep_times_map = self.stop_times_df.groupby("trip_id")["dep_time"].apply(list).to_dict()

        self.stop_to_trips_map = self.stop_times_df.groupby("stop_id")["trip_id"].apply(list).to_dict()
        self.stop_to_dep_times_map = self.stop_times_df.groupby("stop_id")["dep_time"].apply(list).to_dict()

        # Used for realtime updates mappings
        self.trip_to_route_map = self.stop_times_df.groupby("trip_id")["rid"].first().to_dict()

        # Used for journey retrieval
        self.stop_to_stop_name_map = self.stops_df.groupby("stop_id")["stop_name"].first().to_dict()
        self.stop_to_platform_code_map = self.stops_df.groupby("stop_id")["platform_code"].first().to_dict()
        self.trip_stop_to_dep_time_map = self.stop_times_df.groupby(["trip_id", "stop_id"])["dep_time"].first().to_dict()
        self.trip_to_route_name_map = self.stop_times_df.groupby("trip_id")["route_name"].first().to_dict()

    def _discover_possible_transits(self):
        """Discovers possible transits between stops."""

        # Calculating distance between all stops
        dis_mat = haversine_dismat(self.stops_df[["stop_lat", "stop_lon"]].values)
        stop_idxs = np.argwhere((dis_mat < self.MAX_WALK_RADIUS) & (dis_mat > 0))

        # Estimating transit time by haversine-distance (s=vt)
        estimated_transit_time = dis_mat[stop_idxs[:, 0], stop_idxs[:, 1]] / self.WALK_SPEED * 3600 * 2  # penalizing

        # Filling estimated transit times
        self.transits_dct = dict()
        stop_ids = self.stops_df["stop_id"].values[stop_idxs]
        for x, (from_stop_id, to_stop_id) in enumerate(stop_ids):
            if from_stop_id not in self.transits_dct:
                self.transits_dct[from_stop_id] = dict()
            self.transits_dct[from_stop_id][to_stop_id] = estimated_transit_time[x]

        # Filtering known-transfers
        self.transfers_df = self.transfers_df[self.transfers_df["from_stop_id"].isin(self.stops_df["stop_id"]) &
                                              self.transfers_df["to_stop_id"].isin(self.stops_df["stop_id"]) &
                                              (self.transfers_df["from_stop_id"] != self.transfers_df["to_stop_id"])]
        self.transfers_df["min_transfer_time"].fillna(0, inplace=True)

        # Filling known-transfers
        rows = self.transfers_df[["min_transfer_time", "from_stop_id", "to_stop_id"]].values
        for transit_time, from_stop_id, to_stop_id in rows:
            if from_stop_id not in self.transits_dct:
                self.transits_dct[from_stop_id] = dict()
            self.transits_dct[from_stop_id][to_stop_id] = transit_time

    def _filter_active_trips(self):
        """Filters today's active trips."""

        # Converting "hh:mm:ss" time to total-seconds
        self.stop_times_df["arr_time"] = self.stop_times_df["arrival_time"].apply(str_to_seconds)
        self.stop_times_df["dep_time"] = self.stop_times_df["departure_time"].apply(str_to_seconds)

        # Filtering active trips
        self.stop_times_df = self.stop_times_df.merge(self.trips_df[["trip_id", "service_id", "route_id"]])
        self.stop_times_df.loc[self.stop_times_df["dep_time"] < 86400, "date"] = \
            int(str(dt.date.today()).replace("-", ""))
        self.stop_times_df.loc[self.stop_times_df["dep_time"] >= 86400, "date"] = \
            int(str(dt.date.today() + dt.timedelta(days=1)).replace("-", ""))
        self.stop_times_df = self.stop_times_df.merge(self.calendar_dates_df, on=["service_id", "date"])
        self.stops_df = self.stops_df[self.stops_df["stop_id"].isin(self.stop_times_df["stop_id"])]

    def _load_static_data(self):
        """Loads static data from local file."""

        # Loading local static data
        self.stops_df = pd.read_csv("data/stops.txt")
        self.trips_df = pd.read_csv("data/trips.txt")
        self.routes_df = pd.read_csv("data/routes.txt")
        self.transfers_df = pd.read_csv("data/transfers.txt")
        self.stop_times_df = pd.read_csv("data/stop_times.txt")
        self.calendar_dates_df = pd.read_csv("data/calendar_dates.txt")

        # Converting necessary columns to str
        self.stops_df["stop_id"] = self.stops_df["stop_id"].astype(str)
        self.trips_df[["route_id", "trip_id"]] = self.trips_df[["route_id", "trip_id"]].astype(str)
        self.routes_df["route_id"] = self.routes_df["route_id"].astype(str)
        self.transfers_df[["from_stop_id", "to_stop_id"]] = self.transfers_df[["from_stop_id", "to_stop_id"]].astype(str)
        self.stop_times_df[["trip_id", "stop_id"]] = self.stop_times_df[["trip_id", "stop_id"]].astype(str)
