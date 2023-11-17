import math
from numba import njit

@njit(cache=True)
def z_scale(z):
    """
    2^z represents the tile number. Scale that by the number of pixels in each tile.
    """
    PIXELS_PER_TILE = 256
    return 2**z * PIXELS_PER_TILE

def longitude_to_pixel(longitude, zoom):
    return ((longitude + 180) / 360) * z_scale(zoom)


def latitude_to_pixel(latitude, zoom):
    lat_rad = (latitude * math.pi) / 180
    return (
        (1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2
    ) * z_scale(zoom)

@njit(cache=True)
def web_mercator_x_to_pixel_x(x, zoom):
    return (x + (40075016.68557849 / 2.0)) / (40075016.68557849 / (z_scale(zoom)))


@njit(cache=True)
def web_mercator_y_to_pixel_y(y, zoom):
    return (y - (40075016.68557849 / 2.0)) / (40075016.68557849 / (-1 * z_scale(zoom)))

def coordinate_to_pixel(
    input, zoom, return_dict=True, round_int=False, web_mercator=False
):
    """
    Convert coordinate to pixel coordinate
    """
    if web_mercator:
        x = web_mercator_x_to_pixel_x(input[0], zoom)
        y = web_mercator_y_to_pixel_y(input[1], zoom)
    else:
        x = longitude_to_pixel(input[0], zoom)
        y = latitude_to_pixel(input[1], zoom)
    if round_int:
        x = round(x)
        y = round(y)
    if return_dict:
        return {"x": x, "y": y}
    else:
        return [x, y]


def compute_r5_surface(grid: dict, percentile: int) -> np.array:
    """
    Compute single value surface from the grid
    """
    if (
        grid["data"] is None
        or grid["width"] is None
        or grid["height"] is None
        or grid["depth"] is None
    ):
        return None
    travel_time_percentiles = [5, 25, 50, 75, 95]
    percentile_index = travel_time_percentiles.index(percentile)

    if grid["depth"] == 1:
        # if only one percentile is requested, return the grid as is
        surface = grid["data"]
    else:
        grid_percentiles = np.reshape(grid["data"], (grid["depth"], -1))
        surface = grid_percentiles[percentile_index]

    return surface.astype(np.uint8)