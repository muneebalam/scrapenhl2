"""
This method contains utilities for visualization.
"""


def format_number_with_plus(stringnum):
    """
    Converts 0 to 0, -1, to -1, and 1 to +1 (for presentation purposes).
    :param stringnum: int
    :return: str, transformed as specified above.
    """
    if stringnum <= 0:
        return str(stringnum)
    else:
        return '+' + str(stringnum)


def hex_to_rgb(value):
    """Return (red, green, blue) for the color given as #rrggbb."""
    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))


def rgb_to_hex(red, green, blue):
    """Return color as #rrggbb for the given color values."""
    return '#%02x%02x%02x' % (int(red), int(green), int(blue))


def make_color_darker(hex=None, rgb=None, returntype='hex'):
    """
    Makes specified color darker. This is done by converting to rgb and multiplying by 50%.
    :param hex: str. Specify either this or rgb.
    :param rgb: 3-tuple of floats 0-255. Specify either this or hex
    :param returntype: str, 'hex' or 'rgb'
    :return: a hex or rgb color, input color but darker
    """
    if hex is None and rgb is None:
        return
    if hex is not None:
        color = hex_to_rgb(hex)
    else:
        color = rgb

    color = [x * 0.5 for x in color]

    if returntype == 'rgb':
        return color
    return rgb_to_hex(*color)


def make_color_lighter(hex=None, rgb=None, returntype='hex'):
    """
    Makes specified color lighter. This is done by converting to rgb getting closer to 255 by 50%.
    :param hex: str. Specify either this or rgb.
    :param rgb: 3-tuple of floats 0-255. Specify either this or hex
    :param returntype: str, 'hex' or 'rgb'
    :return: a hex or rgb color, input color but lighter
    """
    if hex is None and rgb is None:
        return
    if hex is not None:
        color = hex_to_rgb(hex)
    else:
        color = rgb

    color = [255 - ((255 - x) * 0.5) for x in color]

    if returntype == 'rgb':
        return color
    return rgb_to_hex(*color)
