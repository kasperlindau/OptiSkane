import numpy as np


def str_to_seconds(s):
    """Converts "hh:mm:ss" time to total-seconds."""
    return int(s[:2]) * 3600 + int(s[3:5]) * 60 + int(s[6:8])


def seconds_to_str(s):
    """Converts total-seconds to "hh:mm:ss" time"""
    h = ("0" + str(int(s // 3600)))[-2:]
    m = ("0" + str(int((s - int(h) * 3600) // 60)))[-2:]
    s = ("0" + str(int(s - int(h) * 3600 - int(m) * 60)))[-2:]
    return f"{h}:{m}:{s}"


def haversine(c0, c1s):
    """Calculates haversine distance between a single coordinate c0 and many coordinates c1s."""
    c0, c1s = np.radians(c0), np.radians(c1s)
    a = np.sin((c1s[:, 0] - c0[0]) / 2) ** 2 + np.cos(c0[0]) * np.cos(c1s[:, 0]) * np.sin((c0[1] - c1s[:, 1]) / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return 6371 * c


def haversine_dismat(coords):
    """Calculates haversine-distance-matrix of coordinates."""
    coords = np.radians(coords)
    lat_mat = np.vstack(np.array(np.meshgrid(coords[:, 0], coords[:, 0])).T)
    lon_mat = np.vstack(np.array(np.meshgrid(coords[:, 1], coords[:, 1])).T)
    lat1, lat2 = lat_mat.T
    lon1, lon2 = lon_mat.T
    a = np.sin((lat1 - lat2) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon1 - lon2) / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return (6371 * c).reshape(len(coords), len(coords))





